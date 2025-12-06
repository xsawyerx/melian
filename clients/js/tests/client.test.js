import fs from 'fs';
import net from 'net';
import { MelianClient } from '../src/MelianClient.js';

const DSN = process.env.MELIAN_TEST_DSN || 'unix:///tmp/melian.sock';
const SCHEMA_SPEC = 'table1#0|60|id#0:int,table2#1|60|id#0:int;hostname#1:string';

let client;

beforeAll(async () => {
  await ensureSocketAccessible(DSN, 5000);
  client = await withTimeout(
    MelianClient.create({ dsn: DSN, timeout: 5000 }),
    10000,
    'MelianClient.create timed out; verify the server is responding and schema describe works.'
  );
}, 20000);

afterAll(async () => {
  if (client) {
    await client.close();
  }
});

test('loads schema from describe action', () => {
  const schema = client.getSchema();
  expect(schema.tables).toBeDefined();
  expect(schema.tables.some((table) => table.name === 'table1')).toBe(true);
  expect(schema.tables.some((table) => table.name === 'table2')).toBe(true);
});

test('fetches table1 by id', async () => {
  const { tableId, indexId } = client.resolveIndex('table1', 'id');
  const payload = await client.fetchByInt(tableId, indexId, 5);
  expect(payload).not.toBeNull();
  expect(payload.id).toBe(5);
  expect(payload.name).toBe('item_5');
  expect(payload.category).toBe('alpha');
  expect(payload.value).toBe('VAL_0005');
  expect(payload.active).toBe(1);
});

test('fetches table2 by id and hostname', async () => {
  const { tableId, indexId } = client.resolveIndex('table2', 'id');
  const { indexId: hostIndex } = client.resolveIndex('table2', 'hostname');

  const expected = {
    id: 2,
    hostname: 'host-00002',
    ip: '10.0.2.0',
    status: 'maintenance',
  };

  const byId = await client.fetchByInt(tableId, indexId, 2);
  expect(byId).toEqual(expected);

  const byHost = await client.fetchByString(tableId, hostIndex, Buffer.from('host-00002', 'utf8'));
  expect(byHost).toEqual(expected);
});

test('fetch helpers support named parameters', async () => {
  const byId = await client.fetchByIntFrom('table1', 'id', 5);
  expect(byId).not.toBeNull();
  expect(byId.id).toBe(5);

  const byHostname = await client.fetchByStringFrom('table2', 'hostname', 'host-00002');
  expect(byHostname).not.toBeNull();
  expect(byHostname.id).toBe(2);
});

test('schema spec matches describe result', async () => {
  const specClient = await MelianClient.create({ dsn: DSN, schemaSpec: SCHEMA_SPEC });
  const liveSchema = normalizeSchema(client.getSchema());
  const specSchema = normalizeSchema(specClient.getSchema());
  expect(specSchema).toEqual(liveSchema);
  await specClient.close();
});

test('resolveIndex throws for missing column', () => {
  expect(() => client.resolveIndex('table1', 'unknown_column')).toThrow();
});

test(
  'MelianClient.create rejects quickly for unreachable socket',
  async () => {
    await expect(
      MelianClient.create({ dsn: 'unix:///non-existent.sock', timeout: 100 })
    ).rejects.toThrow();
  },
  5000
);

function normalizeSchema(schema) {
  const tables = [...(schema.tables || [])]
    .map((table) => ({
      ...table,
      indexes: [...(table.indexes || [])].sort((a, b) => a.id - b.id),
    }))
    .sort((a, b) => a.id - b.id);
  return { tables };
}

function parseDsn(dsn) {
  if (dsn.startsWith('unix://')) {
    return { kind: 'unix', path: dsn.slice('unix://'.length) };
  }
  if (dsn.startsWith('tcp://')) {
    const remainder = dsn.slice('tcp://'.length);
    const [host, port] = remainder.split(':');
    return { kind: 'tcp', host, port: Number(port) };
  }
  throw new Error(`Unsupported DSN: ${dsn}`);
}

async function ensureSocketAccessible(dsn, timeoutMs) {
  const parsed = parseDsn(dsn);
  if (parsed.kind === 'unix') {
    await fs.promises.access(parsed.path, fs.constants.R_OK | fs.constants.W_OK);
  }
  await withTimeout(
    new Promise((resolve, reject) => {
      const socket = parsed.kind === 'unix'
        ? net.createConnection(parsed.path)
        : net.createConnection(parsed.port, parsed.host);
      const cleanup = () => {
        socket.removeAllListeners();
        socket.end();
        socket.destroy();
      };
      socket.once('connect', () => {
        cleanup();
        resolve();
      });
      socket.once('error', (err) => {
        cleanup();
        reject(err);
      });
    }),
    timeoutMs,
    `Timed out waiting for socket ${dsn}`
  );
}

function withTimeout(promise, timeoutMs, message) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(message)), timeoutMs);
    promise
      .then((value) => {
        clearTimeout(timer);
        resolve(value);
      })
      .catch((err) => {
        clearTimeout(timer);
        reject(err);
      });
  });
}
