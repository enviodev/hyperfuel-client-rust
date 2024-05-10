@0x9289a56a18f880c5;

struct QueryResponseData {
	blocks @0 :Data;
	transactions @1 :Data;
	receipts @2 :Data;
	inputs @3 :Data;
	outputs @4 :Data;
}

struct QueryResponse {
	archiveHeight @0 :Int64;
	nextBlock @1 :UInt64;
	totalExecutionTime @2 :UInt64;
	data @3 :QueryResponseData;
}
