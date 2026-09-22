#pragma once

namespace duckdb {

class ScalarFunctionSet;

//! Create the local table cache prewarm scalar function overloads.
ScalarFunctionSet GetPrewarmFunction();

} // namespace duckdb
