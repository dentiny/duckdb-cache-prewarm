include_directories(include)
include_directories(${CMAKE_SOURCE_DIR}/third_party)
include_directories(${CMAKE_SOURCE_DIR}/src/include)
include_directories(${DuckDB_SOURCE_DIR}/third_party)
include_directories(${DuckDB_SOURCE_DIR}/third_party/catch)
include_directories(${DuckDB_SOURCE_DIR}/test/include)

file(GLOB_RECURSE CATCHFS_UNITTEST_OBJECTS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

add_executable(unittest_cache_prewarm main.cpp ${CATCHFS_UNITTEST_OBJECTS})

if(NOT WIN32
   AND NOT SUN
   AND NOT ZOS
   AND NOT PREBUILT_BINARY)
  target_link_libraries(unittest_cache_prewarm test_helpers duckdb
                        ${EXTENSION_NAME})
else()
  target_link_libraries(unittest_cache_prewarm test_helpers duckdb_static)
  link_extension_libraries(unittest_cache_prewarm "")
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  target_compile_options(unittest_cache_prewarm PRIVATE -Wthread-safety)
endif()

set_target_properties(
  unittest_cache_prewarm PROPERTIES RUNTIME_OUTPUT_DIRECTORY
                                    "${CMAKE_BINARY_DIR}/test")
