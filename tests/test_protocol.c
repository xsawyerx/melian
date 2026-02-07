#include "test.h"
#include "protocol.h"
#include <arpa/inet.h>
#include <string.h>

TEST(request_header_size) {
  ASSERT_EQ(sizeof(MelianRequestHeader), 8);
}

TEST(response_header_size) {
  ASSERT_EQ(sizeof(MelianResponseHeader), 4);
}

TEST(header_version_constant) {
  ASSERT_EQ(MELIAN_HEADER_VERSION, 0x11);
}

TEST(action_constants) {
  ASSERT_EQ(MELIAN_ACTION_FETCH, 'F');
  ASSERT_EQ(MELIAN_ACTION_DESCRIBE_SCHEMA, 'D');
  ASSERT_EQ(MELIAN_ACTION_GET_STATISTICS, 's');
  ASSERT_EQ(MELIAN_ACTION_QUIT, 'q');
}

TEST(request_header_pack) {
  MelianRequestHeader hdr;
  memset(&hdr, 0, sizeof(hdr));
  hdr.data.version = MELIAN_HEADER_VERSION;
  hdr.data.action = MELIAN_ACTION_FETCH;
  hdr.data.table_id = 2;
  hdr.data.index_id = 1;
  hdr.data.length = htonl(100);

  ASSERT_EQ(hdr.bytes[0], MELIAN_HEADER_VERSION);
  ASSERT_EQ(hdr.bytes[1], 'F');
  ASSERT_EQ(hdr.bytes[2], 2);
  ASSERT_EQ(hdr.bytes[3], 1);
}

TEST(request_header_unpack) {
  MelianRequestHeader hdr;
  hdr.bytes[0] = 0x11;
  hdr.bytes[1] = 'D';
  hdr.bytes[2] = 5;
  hdr.bytes[3] = 3;
  // Set length bytes to network-order 256
  uint32_t net_len = htonl(256);
  memcpy(&hdr.bytes[4], &net_len, 4);

  ASSERT_EQ(hdr.data.version, 0x11);
  ASSERT_EQ(hdr.data.action, 'D');
  ASSERT_EQ(hdr.data.table_id, 5);
  ASSERT_EQ(hdr.data.index_id, 3);
  ASSERT_EQ(ntohl(hdr.data.length), 256);
}

TEST(request_header_length_endian) {
  MelianRequestHeader hdr;
  memset(&hdr, 0, sizeof(hdr));
  hdr.data.length = htonl(0x12345678);
  ASSERT_EQ(ntohl(hdr.data.length), 0x12345678);
}

TEST(response_header_pack) {
  MelianResponseHeader hdr;
  memset(&hdr, 0, sizeof(hdr));
  hdr.data.length = htonl(500);
  // Verify bytes are in network byte order
  ASSERT_EQ(ntohl(hdr.data.length), 500);
  // Manually check first byte for big-endian 500 = 0x000001F4
  ASSERT_EQ(hdr.bytes[0], 0x00);
  ASSERT_EQ(hdr.bytes[1], 0x00);
  ASSERT_EQ(hdr.bytes[2], 0x01);
  ASSERT_EQ(hdr.bytes[3], 0xF4);
}

TEST(value_type_constants) {
  ASSERT_EQ(MELIAN_VALUE_NULL, 0);
  ASSERT_EQ(MELIAN_VALUE_INT64, 1);
  ASSERT_EQ(MELIAN_VALUE_FLOAT64, 2);
  ASSERT_EQ(MELIAN_VALUE_BYTES, 3);
  ASSERT_EQ(MELIAN_VALUE_DECIMAL, 4);
  ASSERT_EQ(MELIAN_VALUE_BOOL, 5);
}

TEST_MAIN_BEGIN
  TEST_RUN(request_header_size);
  TEST_RUN(response_header_size);
  TEST_RUN(header_version_constant);
  TEST_RUN(action_constants);
  TEST_RUN(request_header_pack);
  TEST_RUN(request_header_unpack);
  TEST_RUN(request_header_length_endian);
  TEST_RUN(response_header_pack);
  TEST_RUN(value_type_constants);
TEST_MAIN_END
