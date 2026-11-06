package main

// Compile-time checks: a signature drift in a provider capability would
// otherwise silently disable the optional step through a failed type assertion.
var (
	_ Provider             = (*nativeProvider)(nil)
	_ Session              = (*nativeSession)(nil)
	_ CatalogSource        = (*nativeSession)(nil)
	_ Subscriber           = (*nativeSession)(nil)
	_ SecuritiesInfoSource = (*nativeSession)(nil)
	_ HistorySource        = (*nativeSession)(nil)
	_ PositionSource       = (*nativeSession)(nil)

	_ Provider             = (*grpcProvider)(nil)
	_ Session              = (*grpcSession)(nil)
	_ CatalogSource        = (*grpcSession)(nil)
	_ Subscriber           = (*grpcSession)(nil)
	_ SecuritiesInfoSource = (*grpcSession)(nil)
	_ HistorySource        = (*grpcSession)(nil)
	_ PositionSource       = (*grpcSession)(nil)
)
