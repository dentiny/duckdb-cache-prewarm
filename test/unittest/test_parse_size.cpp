#include "catch/catch.hpp"

#include "utils/include/parse_size.hpp"

using namespace duckdb; // NOLINT

namespace {

TEST_CASE("ParseSizeLimit - plain integers (treated as bytes)", "[ParseSize]") {
	REQUIRE(ParseSizeLimit("0") == 0);
	REQUIRE(ParseSizeLimit("1") == 1);
	REQUIRE(ParseSizeLimit("1000000") == 1000000);
	REQUIRE(ParseSizeLimit("999999999") == 999999999);
}

TEST_CASE("ParseSizeLimit - floating point values (treated as bytes)", "[ParseSize]") {
	REQUIRE(ParseSizeLimit("1.5") == 1);
	REQUIRE(ParseSizeLimit("1000.9") == 1000);
}

TEST_CASE("ParseSizeLimit - bytes suffix", "[ParseSize]") {
	REQUIRE(ParseSizeLimit("100bytes") == 100);
	REQUIRE(ParseSizeLimit("100b") == 100);
	REQUIRE(ParseSizeLimit("100B") == 100);
	REQUIRE(ParseSizeLimit("100BYTES") == 100);
	REQUIRE(ParseSizeLimit("100Bytes") == 100);
}

TEST_CASE("ParseSizeLimit - kilobytes (SI: 1KB = 1000 bytes)", "[ParseSize]") {
	REQUIRE(ParseSizeLimit("1kb") == 1000);
	REQUIRE(ParseSizeLimit("1KB") == 1000);
	REQUIRE(ParseSizeLimit("1Kb") == 1000);
	REQUIRE(ParseSizeLimit("10kb") == 10000);
}

TEST_CASE("ParseSizeLimit - megabytes (SI: 1MB = 1000000 bytes)", "[ParseSize]") {
	REQUIRE(ParseSizeLimit("1mb") == 1000000);
	REQUIRE(ParseSizeLimit("1MB") == 1000000);
	REQUIRE(ParseSizeLimit("1Mb") == 1000000);
	REQUIRE(ParseSizeLimit("100mb") == 100000000);
}

TEST_CASE("ParseSizeLimit - gigabytes (SI: 1GB = 1000000000 bytes)", "[ParseSize]") {
	REQUIRE(ParseSizeLimit("1gb") == 1000000000);
	REQUIRE(ParseSizeLimit("1GB") == 1000000000);
	REQUIRE(ParseSizeLimit("1Gb") == 1000000000);
	REQUIRE(ParseSizeLimit("2gb") == 2000000000ULL);
}

TEST_CASE("ParseSizeLimit - terabytes (SI: 1TB = 1000000000000 bytes)", "[ParseSize]") {
	REQUIRE(ParseSizeLimit("1tb") == 1000000000000ULL);
	REQUIRE(ParseSizeLimit("1TB") == 1000000000000ULL);
}

TEST_CASE("ParseSizeLimit - invalid values", "[ParseSize]") {
	REQUIRE_THROWS(ParseSizeLimit(""));
	REQUIRE_THROWS(ParseSizeLimit("abc"));
	REQUIRE_THROWS(ParseSizeLimit("mb"));
}

} // namespace
