<?php

declare(strict_types=1);

namespace Melian;

use InvalidArgumentException;
use RuntimeException;

/**
 * Melian PHP client library.
 *
 * Usage example:
 *
 * $client = new Client([
 *     'dsn' => 'unix:///tmp/melian.sock',
 *     // Optional: 'schema_spec' => 'table1#0|60|id#0:int'
 * ]);
 * [$tableId, $indexId] = $client->resolveIndex('table2', 'hostname');
 * $row = $client->fetchByString($tableId, $indexId, 'host-00042');
 */
final class Client
{
    private const HEADER_VERSION = 0x11;
    private const ACTION_FETCH = 0x46;    // 'F'
    private const ACTION_DESCRIBE = 0x44; // 'D'

    private string $dsn;
    private float $timeout;

    /** @var resource|null */
    private $socket = null;

    /** @var array<string, mixed> */
    private array $schema;

    /**
     * @param array{
     *   dsn?: string,
     *   timeout?: float,
     *   schema?: array<string,mixed>,
     *   schema_spec?: string,
     *   schema_file?: string
     * } $options
     */
    public function __construct(array $options = [])
    {
        $this->dsn = $options['dsn'] ?? 'unix:///tmp/melian.sock';
        $this->timeout = isset($options['timeout']) ? (float)$options['timeout'] : 1.0;
        $this->schema = $this->bootstrapSchema($options);
    }

    public function __destruct()
    {
        $this->disconnect();
    }

    /**
     * Returns the parsed schema (tables + indexes).
     *
     * @return array<string, mixed>
     */
    public function schema(): array
    {
        return $this->schema;
    }

    /**
     * Fetches a raw payload for the provided table/index identifiers.
     */
    public function fetchRaw(int $tableId, int $indexId, string $key): string
    {
        if ($tableId < 0 || $tableId > 255 || $indexId < 0 || $indexId > 255) {
            throw new InvalidArgumentException('Table/index identifiers must be between 0 and 255.');
        }
        return $this->send(self::ACTION_FETCH, $tableId, $indexId, $key);
    }

    /**
     * Fetches JSON (decoded as associative array) for a table/index/key triple.
     *
     * @return array<string, mixed>|null
     */
    public function fetchByString(int $tableId, int $indexId, string $key): ?array
    {
        $payload = $this->fetchRaw($tableId, $indexId, $key);
        if ($payload === '') {
            return null;
        }

        $decoded = json_decode($payload, true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new RuntimeException('Failed to decode JSON payload: ' . json_last_error_msg());
        }
        return $decoded;
    }

    /**
     * Helper for integer identifiers encoded as 32-bit LE.
     *
     * @return array<string, mixed>|null
     */
    public function fetchByInt(int $tableId, int $indexId, int $id): ?array
    {
        $key = pack('V', $id);
        return $this->fetchByString($tableId, $indexId, $key);
    }

    /**
     * Resolves identifiers and fetches a row by a string column.
     *
     * @return array<string,mixed>|null
     */
    public function fetchByStringFrom(string $tableName, string $column, string $key): ?array
    {
        $ids = $this->resolveIndex($tableName, $column);
        return $this->fetchByString($ids['table_id'], $ids['index_id'], $key);
    }

    /**
     * Resolves identifiers and fetches by a numeric column encoded as LE.
     *
     * @return array<string,mixed>|null
     */
    public function fetchByIntFrom(string $tableName, string $column, int $id): ?array
    {
        $ids = $this->resolveIndex($tableName, $column);
        return $this->fetchByInt($ids['table_id'], $ids['index_id'], $id);
    }

    /**
     * Resolves table/index identifiers from human-readable names.
     *
     * @return array{table_id:int,index_id:int}
     */
    public function resolveIndex(string $tableName, string $indexColumn): array
    {
        foreach ($this->schema['tables'] as $table) {
            if (($table['name'] ?? null) !== $tableName) {
                continue;
            }
            foreach ($table['indexes'] ?? [] as $index) {
                if (($index['column'] ?? null) === $indexColumn) {
                    return [
                        'table_id' => (int)$table['id'],
                        'index_id' => (int)$index['id'],
                    ];
                }
            }
        }

        throw new RuntimeException(sprintf(
            'Unable to resolve index for table "%s" column "%s".',
            $tableName,
            $indexColumn
        ));
    }

    /**
     * Fetches the live schema from the server via MELIAN_ACTION_DESCRIBE.
     *
     * @return array<string, mixed>
     */
    public function describeSchema(): array
    {
        $payload = $this->send(self::ACTION_DESCRIBE, 0, 0, '');
        if ($payload === '') {
            throw new RuntimeException('Server returned empty schema description.');
        }
        $decoded = json_decode($payload, true);
        if (json_last_error() !== JSON_ERROR_NONE || !is_array($decoded)) {
            throw new RuntimeException('Failed to decode schema JSON: ' . json_last_error_msg());
        }
        return $decoded;
    }

    /**
     * Closes the underlying socket.
     */
    public function disconnect(): void
    {
        if (is_resource($this->socket)) {
            fclose($this->socket);
            $this->socket = null;
        }
    }

    /**
     * Sends a protocol request and returns the raw payload.
     */
    private function send(int $action, int $tableId, int $indexId, string $payload): string
    {
        $this->ensureConnected();
        $header = pack(
            'CCCCN',
            self::HEADER_VERSION,
            $action,
            $tableId,
            $indexId,
            strlen($payload)
        );
        $this->writeAll($header . $payload);

        $lengthBytes = $this->readExactly(4);
        $length = unpack('Nlen', $lengthBytes)['len'];
        if ($length === 0) {
            return '';
        }

        return $this->readExactly($length);
    }

    /**
     * Ensures the socket connection is open.
     */
    private function ensureConnected(): void
    {
        if (is_resource($this->socket)) {
            return;
        }

        $errno = 0;
        $errstr = '';
        $socket = @stream_socket_client(
            $this->dsn,
            $errno,
            $errstr,
            $this->timeout,
            STREAM_CLIENT_CONNECT
        );
        if (!is_resource($socket)) {
            throw new RuntimeException(sprintf('Failed to connect to %s: %s (%d)', $this->dsn, $errstr, $errno));
        }
        stream_set_blocking($socket, true);
        $this->socket = $socket;
    }

    private function writeAll(string $buffer): void
    {
        $offset = 0;
        $length = strlen($buffer);
        while ($offset < $length) {
            $written = fwrite($this->socket, substr($buffer, $offset));
            if ($written === false || $written === 0) {
                throw new RuntimeException('Socket write failed.');
            }
            $offset += $written;
        }
    }

    private function readExactly(int $length): string
    {
        $data = '';
        while (strlen($data) < $length) {
            $chunk = fread($this->socket, $length - strlen($data));
            if ($chunk === false || $chunk === '') {
                throw new RuntimeException('Socket read failed or EOF encountered.');
            }
            $data .= $chunk;
        }
        return $data;
    }

    /**
     * @param array<string,mixed> $options
     *
     * @return array<string,mixed>
     */
    private function bootstrapSchema(array $options): array
    {
        $schemaSources = array_filter([
            array_key_exists('schema', $options) ? 'schema' : null,
            array_key_exists('schema_spec', $options) ? 'schema_spec' : null,
            array_key_exists('schema_file', $options) ? 'schema_file' : null,
        ]);

        if (count($schemaSources) > 1) {
            throw new InvalidArgumentException('Provide at most one of: schema, schema_spec, schema_file.');
        }

        if (isset($options['schema'])) {
            if (!is_array($options['schema'])) {
                throw new InvalidArgumentException('schema must be an associative array.');
            }
            return $options['schema'];
        }

        if (isset($options['schema_spec'])) {
            return $this->loadSchemaFromSpec((string)$options['schema_spec']);
        }

        if (isset($options['schema_file'])) {
            return $this->loadSchemaFromFile((string)$options['schema_file']);
        }

        return $this->describeSchema();
    }

    /**
     * Parses a schema specification string.
     *
     * Format: name#id|period|column#idx[:type];...
     *
     * @return array<string,mixed>
     */
    private function loadSchemaFromSpec(string $spec): array
    {
        if (trim($spec) === '') {
            throw new InvalidArgumentException('schema_spec cannot be empty.');
        }

        $tables = [];
        foreach (explode(',', $spec) as $chunk) {
            $chunk = trim($chunk);
            if ($chunk === '') {
                continue;
            }

            [$tablePart, $periodPart, $columnsPart] = array_pad(explode('|', $chunk, 3), 3, '');
            [$name, $id] = array_pad(explode('#', $tablePart, 2), 2, null);
            if ($name === null || $id === null || $name === '' || $id === '') {
                throw new InvalidArgumentException(sprintf('Invalid table spec chunk "%s".', $chunk));
            }

            $table = [
                'name' => $name,
                'id' => (int)$id,
                'period' => ($periodPart !== '') ? (int)$periodPart : 0,
                'indexes' => [],
            ];

            if ($columnsPart === '') {
                throw new InvalidArgumentException(sprintf('Table "%s" must define at least one index.', $name));
            }

            foreach (explode(';', $columnsPart) as $columnSpec) {
                $columnSpec = trim($columnSpec);
                if ($columnSpec === '') {
                    continue;
                }
                [$columnWithId, $type] = array_pad(explode(':', $columnSpec, 2), 2, 'int');
                [$columnName, $columnId] = array_pad(explode('#', $columnWithId, 2), 2, null);
                if ($columnName === null || $columnId === null || $columnName === '' || $columnId === '') {
                    throw new InvalidArgumentException(sprintf(
                        'Invalid index spec "%s" for table "%s".',
                        $columnSpec,
                        $name
                    ));
                }
                $table['indexes'][] = [
                    'column' => $columnName,
                    'id' => (int)$columnId,
                    'type' => $type,
                ];
            }

            if (empty($table['indexes'])) {
                throw new InvalidArgumentException(sprintf('Table "%s" has no indexes.', $name));
            }

            $tables[] = $table;
        }

        if (empty($tables)) {
            throw new InvalidArgumentException('Parsed schema_spec produced no tables.');
        }

        return ['tables' => $tables];
    }

    /**
     * Loads schema JSON from disk.
     *
     * @return array<string,mixed>
     */
    private function loadSchemaFromFile(string $path): array
    {
        if (!is_file($path)) {
            throw new InvalidArgumentException(sprintf('Schema file "%s" does not exist.', $path));
        }
        $contents = file_get_contents($path);
        if ($contents === false) {
            throw new RuntimeException(sprintf('Failed to read schema file "%s".', $path));
        }
        $decoded = json_decode($contents, true);
        if (json_last_error() !== JSON_ERROR_NONE || !is_array($decoded)) {
            throw new RuntimeException('Failed to parse schema JSON file: ' . json_last_error_msg());
        }
        return $decoded;
    }
}
