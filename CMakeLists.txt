cmake_minimum_required(VERSION 3.10)

# Set extension name here
set(TARGET_NAME cache_prewarm)
set(CMAKE_CXX_STANDARD 14)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)
include_directories(duck-read-cache-fs/src/include)
include_directories(duck-read-cache-fs/src/utils/include)
include_directories(duck-read-cache-fs/duckdb-httpfs/src/include)
include_directories(duck-read-cache-fs/duckdb/third_party/httplib)

set(EXTENSION_SOURCES
    src/cache_prewarm_extension.cpp
    src/core/block_collector.cpp
    src/core/buffer_prewarm_strategy.cpp
    src/core/os_prefetch.cpp
    src/core/prefetch_prewarm_strategy.cpp
    src/core/prewarm_strategy.cpp
    src/core/prewarm_strategy_factory.cpp
    src/core/read_prewarm_strategy.cpp
    src/core/remote_block_collector.cpp
    src/core/remote_prewarm_strategy.cpp
    src/functions/prewarm_function.cpp
    src/functions/prewarm_remote_function.cpp

    # duck-read-cache-fs sources
    duck-read-cache-fs/src/cache_httpfs_extension.cpp
    duck-read-cache-fs/src/cache_filesystem_config.cpp
    duck-read-cache-fs/src/cache_httpfs_instance_state.cpp
    duck-read-cache-fs/src/cache_filesystem.cpp
    duck-read-cache-fs/src/cache_entry_info.cpp
    duck-read-cache-fs/src/base_profile_collector.cpp
    duck-read-cache-fs/src/noop_profile_collector.cpp
    duck-read-cache-fs/src/temp_profile_collector.cpp
    duck-read-cache-fs/src/noop_cache_reader.cpp
    duck-read-cache-fs/src/in_memory_cache_reader.cpp
    duck-read-cache-fs/src/disk_cache_reader.cpp
    duck-read-cache-fs/src/cache_read_chunk.cpp
    duck-read-cache-fs/src/io_operations.cpp
    duck-read-cache-fs/src/cache_exclusion_manager.cpp
    duck-read-cache-fs/src/cache_exclusion_utils.cpp
    duck-read-cache-fs/src/histogram.cpp
    duck-read-cache-fs/src/disk_cache_util.cpp
    duck-read-cache-fs/src/in_mem_cache_block.cpp
    duck-read-cache-fs/src/utils/filesystem_utils.cpp
    duck-read-cache-fs/src/utils/thread_utils.cpp
    duck-read-cache-fs/src/utils/thread_pool.cpp
    duck-read-cache-fs/src/utils/chunk_utils.cpp
    duck-read-cache-fs/src/utils/time_utils.cpp
    duck-read-cache-fs/src/utils/fake_filesystem.cpp
    duck-read-cache-fs/src/utils/mock_filesystem.cpp
    duck-read-cache-fs/src/utils/scoped_directory.cpp
    duck-read-cache-fs/src/utils/hash_utils.cpp
    duck-read-cache-fs/src/utils/url_utils.cpp
    duck-read-cache-fs/src/cache_status_query_function.cpp
    duck-read-cache-fs/src/extension_config_query_function.cpp
    duck-read-cache-fs/src/filesystem_status_query_function.cpp
    duck-read-cache-fs/duckdb-httpfs/src/hash_functions.cpp
    duck-read-cache-fs/duckdb-httpfs/src/httpfs_extension.cpp
    duck-read-cache-fs/duckdb-httpfs/src/httpfs.cpp
    duck-read-cache-fs/duckdb-httpfs/src/s3fs.cpp
    duck-read-cache-fs/duckdb-httpfs/src/hffs.cpp
    duck-read-cache-fs/duckdb-httpfs/src/httpfs_curl_client.cpp
    duck-read-cache-fs/duckdb-httpfs/src/httpfs_httplib_client.cpp
    duck-read-cache-fs/duckdb-httpfs/src/http_state.cpp
    duck-read-cache-fs/duckdb-httpfs/src/create_secret_functions.cpp
    duck-read-cache-fs/duckdb-httpfs/src/crypto.cpp)

find_package(OpenSSL REQUIRED)
find_package(CURL REQUIRED)

include_directories(${OPENSSL_INCLUDE_DIR})

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto CURL::libcurl)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto CURL::libcurl)

if(MINGW OR WIN32)
  target_link_libraries(${EXTENSION_NAME} crypt32)
  target_link_libraries(${LOADABLE_EXTENSION_NAME} crypt32)
endif()

if(${BUILD_UNITTESTS})
  add_subdirectory(test/unittest)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

if("${BUILD_BENCHMARK}" STREQUAL "1")
  add_executable(clickbench bench/clickbench.cpp)
  target_link_libraries(clickbench duckdb ${EXTENSION_NAME})
  set_target_properties(clickbench PROPERTIES RUNTIME_OUTPUT_DIRECTORY
                                              "${CMAKE_BINARY_DIR}/bench")
  install(TARGETS clickbench RUNTIME DESTINATION "${INSTALL_BIN_DIR}")
endif()
