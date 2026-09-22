#pragma once

namespace duckdb {

class ScalarFunctionSet;

//! Create the remote file cache prewarm scalar function overloads.
ScalarFunctionSet GetPrewarmRemoteFunction();

} // namespace duckdb
