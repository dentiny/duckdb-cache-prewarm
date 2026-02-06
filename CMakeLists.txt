cmake_minimum_required(VERSION 3.10)

# Set extension name here
set(TARGET_NAME cache_prewarm)
set(CMAKE_CXX_STANDARD 14)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be
# added in ./vcpkg.json and then used in cmake with find_package. Feel free to
# remove or replace with other dependencies. Note that it should also be removed
# from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/cache_prewarm_extension.cpp
    src/core/block_collector.cpp
    src/core/buffer_prewarm_strategy.cpp
    src/core/prefetch_prewarm_strategy.cpp
    src/core/prewarm_strategy.cpp
    src/core/prewarm_strategy_factory.cpp
    src/core/read_prewarm_strategy.cpp
    src/functions/prewarm_function.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

if ("${BUILD_BENCHMARK}" STREQUAL "1")
	add_executable(clickbench bench/clickbench.cpp)
	target_link_libraries(clickbench duckdb ${EXTENSION_NAME})
	set_target_properties(clickbench PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bench")
	install(TARGETS clickbench RUNTIME DESTINATION "${INSTALL_BIN_DIR}")
endif()
