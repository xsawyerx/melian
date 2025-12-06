<?php

declare(strict_types=1);

namespace Melian\Tests;

use Melian\Client;
use PHPUnit\Framework\TestCase;

final class ClientTest extends TestCase
{
    private const DEFAULT_DSN = 'unix:///tmp/melian.sock';

    private Client $client;

    protected function setUp(): void
    {
        $dsn = getenv('MELIAN_TEST_DSN') ?: self::DEFAULT_DSN;
        $this->client = new Client([
            'dsn' => $dsn,
            'timeout' => 1.0,
        ]);
    }

    protected function tearDown(): void
    {
        $this->client->disconnect();
    }

    public function testConnectionLoadsSchema(): void
    {
        $schema = $this->client->schema();
        $this->assertIsArray($schema);
        $this->assertArrayHasKey('tables', $schema);
        $this->assertNotEmpty($schema['tables']);

        $tableNames = array_column($schema['tables'], 'name');
        $this->assertContains('table1', $tableNames);
        $this->assertContains('table2', $tableNames);
    }

    public function testFetchTable1ById(): void
    {
        [$tableId, $indexId] = $this->resolveIndex('table1', 'id');
        $recordId = 5;
        $payload = $this->client->fetchByInt($tableId, $indexId, $recordId);

        $this->assertIsArray($payload);
        $this->assertSame($recordId, $payload['id']);
        $this->assertSame('item_5', $payload['name']);
        $this->assertSame('alpha', $payload['category']);
        $this->assertSame('VAL_0005', $payload['value']);
        $this->assertSame(1, $payload['active']);
    }

    public function testTable2ByIdAndHostname(): void
    {
        [$tableId, $idIndex] = $this->resolveIndex('table2', 'id');
        [$ignoredTable, $hostIndex] = $this->resolveIndex('table2', 'hostname');

        $recordId = 2;
        $hostname = 'host-00002';

        $expected = [
            'id' => $recordId,
            'hostname' => $hostname,
            'ip' => '10.0.2.0',
            'status' => 'maintenance',
        ];

        $byId = $this->client->fetchByInt($tableId, $idIndex, $recordId);
        $this->assertSame($expected, $byId);

        $byHostname = $this->client->fetchByString($tableId, $hostIndex, $hostname);
        $this->assertSame($expected, $byHostname);
    }

    public function testNamedFetchHelpers(): void
    {
        $byId = $this->client->fetchByIntFrom('table1', 'id', 5);
        $this->assertNotNull($byId);
        $this->assertSame(5, $byId['id']);

        $byHostname = $this->client->fetchByStringFrom('table2', 'hostname', 'host-00002');
        $this->assertNotNull($byHostname);
        $this->assertSame(2, $byHostname['id']);
    }

    public function testSchemaSpecAndDescribeProduceSameStructure(): void
    {
        $spec = 'table1#0|60|id#0:int,table2#1|60|id#0:int;hostname#1:string';
        $schemaFromLive = $this->client->schema();

        $clientFromSpec = new Client([
            'dsn' => getenv('MELIAN_TEST_DSN') ?: self::DEFAULT_DSN,
            'schema_spec' => $spec,
        ]);

        $this->assertSame(
            $this->normalizeSchema($schemaFromLive),
            $this->normalizeSchema($clientFromSpec->schema())
        );
    }

    public function testResolveIndexThrowsForUnknownIndex(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->client->resolveIndex('table1', 'nonexistent_column');
    }

    /**
     * @return array{int,int}
     */
    private function resolveIndex(string $table, string $index): array
    {
        $resolved = $this->client->resolveIndex($table, $index);
        return [$resolved['table_id'], $resolved['index_id']];
    }

    /**
     * Sorts tables and indexes so that comparisons ignore key ordering.
     *
     * @param array<string,mixed> $schema
     * @return array<string,mixed>
     */
    private function normalizeSchema(array $schema): array
    {
        if (!isset($schema['tables']) || !is_array($schema['tables'])) {
            return $schema;
        }

        $tables = array_values($schema['tables']);
        usort($tables, static fn ($a, $b) => ($a['id'] ?? 0) <=> ($b['id'] ?? 0));
        foreach ($tables as &$table) {
            if (isset($table['indexes']) && is_array($table['indexes'])) {
                $indexes = array_values($table['indexes']);
                usort($indexes, static fn ($a, $b) => ($a['id'] ?? 0) <=> ($b['id'] ?? 0));
                foreach ($indexes as &$index) {
                    if (is_array($index)) {
                        ksort($index);
                    }
                }
                $table['indexes'] = $indexes;
            }
            ksort($table);
        }
        $schema['tables'] = $tables;
        ksort($schema);
        return $schema;
    }
}
