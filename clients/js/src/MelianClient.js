import net from 'net';
import fs from 'fs';

const HEADER_VERSION = 0x11;
const ACTION_FETCH = 'F'.charCodeAt(0);
const ACTION_DESCRIBE = 'D'.charCodeAt(0);

export class MelianClient {
  constructor({
    dsn = 'unix:///tmp/melian.sock',
    timeout = 1000,
    schema = null,
    schemaSpec = null,
    schemaFile = null,
  } = {}) {
    this.dsn = this.#parseDsn(dsn);
    this.timeout = timeout;
    this.socket = null;
    this.schema = null;
    this.schemaOptions = { schema, schemaSpec, schemaFile };
  }

  static async create(options = {}) {
    const client = new MelianClient(options);
    await client.#initializeSchema();
    return client;
  }

  async close() {
    if (this.socket) {
      this.socket.end();
      this.socket.destroy();
      this.socket = null;
    }
  }

  getSchema() {
    if (!this.schema) {
      throw new Error('Schema not loaded. Call MelianClient.create() or refreshSchema().');
    }
    return this.schema;
  }

  async refreshSchema() {
    this.schema = await this.describeSchema();
    return this.schema;
  }

  async describeSchema() {
    const payload = await this.#send(ACTION_DESCRIBE, 0, 0, Buffer.alloc(0));
    if (!payload.length) {
      throw new Error('Server returned empty schema description');
    }
    const decoded = JSON.parse(payload.toString('utf8'));
    if (typeof decoded !== 'object' || decoded === null) {
      throw new Error('Schema description must be a JSON object');
    }
    return decoded;
  }

  async fetchRaw(tableId, indexId, keyBuffer) {
    if (tableId < 0 || tableId > 255 || indexId < 0 || indexId > 255) {
      throw new RangeError('tableId/indexId must be between 0 and 255');
    }
    return this.#send(ACTION_FETCH, tableId, indexId, keyBuffer);
  }

  async fetchByString(tableId, indexId, keyBuffer) {
    const payload = await this.fetchRaw(tableId, indexId, keyBuffer);
    if (!payload.length) {
      return null;
    }
    return JSON.parse(payload.toString('utf8'));
  }

  async fetchByInt(tableId, indexId, id) {
    const key = Buffer.allocUnsafe(4);
    key.writeUInt32LE(id, 0);
    return this.fetchByString(tableId, indexId, key);
  }

  async fetchByStringFrom(tableName, column, key) {
    const { tableId, indexId } = this.resolveIndex(tableName, column);
    const keyBuffer = this.#coerceKeyBuffer(key);
    return this.fetchByString(tableId, indexId, keyBuffer);
  }

  async fetchByIntFrom(tableName, column, id) {
    const { tableId, indexId } = this.resolveIndex(tableName, column);
    return this.fetchByInt(tableId, indexId, id);
  }

  resolveIndex(tableName, column) {
    if (!this.schema) {
      throw new Error('Schema not loaded.');
    }
    for (const table of this.schema.tables || []) {
      if (table.name !== tableName) continue;
      for (const index of table.indexes || []) {
        if (index.column === column) {
          return { tableId: table.id, indexId: index.id };
        }
      }
    }
    throw new Error(`Unable to resolve index for ${tableName}.${column}`);
  }

  async #initializeSchema() {
    this.schema = await this.#bootstrapSchema(this.schemaOptions);
  }

  async #ensureConnected() {
    if (this.socket) {
      return this.socket;
    }

    const socket = this.dsn.kind === 'unix'
      ? net.createConnection(this.dsn.path)
      : net.createConnection(this.dsn.port, this.dsn.host);

    this.socket = socket;

    await new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        socket.destroy(new Error('Connection timed out'));
      }, this.timeout);

      const cleanup = () => {
        socket.removeListener('error', onError);
        socket.removeListener('connect', onConnect);
        clearTimeout(timeoutId);
      };
      const onError = (err) => {
        cleanup();
        reject(err);
      };
      const onConnect = () => {
        cleanup();
        resolve();
      };
      if (socket.readyState === 'open') {
        cleanup();
        resolve();
        return;
      }
      socket.once('error', onError);
      socket.once('connect', onConnect);
    });

    return socket;
  }

  async #send(action, tableId, indexId, payload) {
    const socket = await this.#ensureConnected();
    const header = Buffer.alloc(8);
    header.writeUInt8(HEADER_VERSION, 0);
    header.writeUInt8(action, 1);
    header.writeUInt8(tableId, 2);
    header.writeUInt8(indexId, 3);
    header.writeUInt32BE(payload.length, 4);

    await new Promise((resolve, reject) => {
      socket.write(Buffer.concat([header, payload]), (err) => {
        if (err) reject(err);
        else resolve();
      });
    });

    const lengthBuffer = await this.#readExact(socket, 4);
    const length = lengthBuffer.readUInt32BE(0);
    if (length === 0) {
      return Buffer.alloc(0);
    }
    return this.#readExact(socket, length);
  }

  #readExact(socket, size) {
    const chunks = [];
    let remaining = size;
    return new Promise((resolve, reject) => {
      const onReadable = () => {
        while (remaining > 0) {
          const chunk = socket.read(remaining);
          if (!chunk) break;
          chunks.push(chunk);
          remaining -= chunk.length;
        }
        if (remaining === 0) {
          cleanup();
          resolve(Buffer.concat(chunks, size));
        }
      };

      const onEnd = () => {
        cleanup();
        reject(new Error('Socket closed before reading response'));
      };

      const onError = (err) => {
        cleanup();
        reject(err);
      };

      const cleanup = () => {
        socket.removeListener('readable', onReadable);
        socket.removeListener('end', onEnd);
        socket.removeListener('error', onError);
      };

      socket.on('readable', onReadable);
      socket.on('end', onEnd);
      socket.on('error', onError);
      onReadable();
    });
  }

  async #bootstrapSchema({ schema, schemaSpec, schemaFile }) {
    const provided = [schema, schemaSpec, schemaFile].filter(Boolean);
    if (provided.length > 1) {
      throw new Error('Provide at most one of schema, schemaSpec, schemaFile');
    }
    if (schema) {
      return schema;
    }
    if (schemaSpec) {
      return this.#loadSchemaFromSpec(schemaSpec);
    }
    if (schemaFile) {
      return this.#loadSchemaFromFile(schemaFile);
    }
    return this.describeSchema();
  }

  #loadSchemaFromFile(path) {
    const contents = fs.readFileSync(path, 'utf8');
    return JSON.parse(contents);
  }

  #loadSchemaFromSpec(spec) {
    const tables = [];
    for (const chunk of spec.split(',')) {
      const trimmed = chunk.trim();
      if (!trimmed) continue;
      const [tablePart, periodPart = '', columnsPart = ''] = trimmed.split('|');
      const [name, id] = this.#splitWithHash(tablePart, 'table');
      if (!columnsPart) {
        throw new Error(`Table ${name} must define at least one index`);
      }
      const table = {
        name,
        id: Number(id),
        period: periodPart ? Number(periodPart) : 0,
        indexes: [],
      };
      for (const idxSpec of columnsPart.split(';')) {
        const specTrimmed = idxSpec.trim();
        if (!specTrimmed) continue;
        const [columnWithId, type = 'int'] = specTrimmed.split(':');
        const [column, columnId] = this.#splitWithHash(columnWithId, 'index');
        table.indexes.push({ column, id: Number(columnId), type });
      }
      tables.push(table);
    }
    if (!tables.length) {
      throw new Error('schemaSpec produced no tables');
    }
    return { tables };
  }

  #splitWithHash(value, label) {
    const hashPos = value.indexOf('#');
    if (hashPos === -1) {
      throw new Error(`Missing # delimiter for ${label} specification: ${value}`);
    }
    const name = value.slice(0, hashPos).trim();
    const ident = value.slice(hashPos + 1).trim();
    if (!name || !ident) {
      throw new Error(`Invalid ${label} specification: ${value}`);
    }
    return [name, ident];
  }

  #coerceKeyBuffer(key) {
    if (Buffer.isBuffer(key)) {
      return key;
    }
    if (typeof key === 'string') {
      return Buffer.from(key, 'utf8');
    }
    if (key instanceof Uint8Array) {
      return Buffer.from(key);
    }
    throw new TypeError('key must be a Buffer, string, or Uint8Array');
  }

  #parseDsn(dsn) {
    if (dsn.startsWith('unix://')) {
      return { kind: 'unix', path: dsn.slice('unix://'.length) };
    }
    if (dsn.startsWith('tcp://')) {
      const remainder = dsn.slice('tcp://'.length);
      const [host, port] = remainder.split(':');
      if (!host || !port) {
        throw new Error('TCP DSN must include host:port');
      }
      return { kind: 'tcp', host, port: Number(port) };
    }
    throw new Error('Unsupported DSN: ' + dsn);
  }
}
