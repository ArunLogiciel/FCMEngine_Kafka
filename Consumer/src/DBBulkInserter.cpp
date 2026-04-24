#pragma once
#include <fstream>
#include <sstream>
#include "../FeeModule/DBBulkInserter.h"
#include "../FeeModule/logger/logger.hpp"
#include <filesystem>
#include <ctime>
#include <chrono>
#include "../FeeModule/framework.h"

std::string BulkInserter::getDate()
{
	// Get the current time using chrono library
	auto now = std::chrono::system_clock::now();
	auto duration = now.time_since_epoch();

	// Extract milliseconds from duration
	auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count() % 1000;

	// Convert time_t to tm
	time_t timer = std::chrono::system_clock::to_time_t(now);
	tm* buffer = new tm;
	localtime_s(buffer, &timer);

	// Create a string for date
	std::string date = std::to_string(buffer->tm_mday) + "-" +
		std::to_string(buffer->tm_mon + 1) + "-" +
		std::to_string(buffer->tm_year + 1900);

	// Create a string for time including milliseconds
	std::string time = std::to_string(buffer->tm_hour) +
		std::to_string(buffer->tm_min) +
		std::to_string(buffer->tm_sec) +
		std::to_string(milliseconds);

	delete buffer; // Don't forget to delete the allocated buffer to prevent memory leak

	// Combine date and time strings to get the whole timestamp
	std::string timestamp = "EOD-" + date + "-" + time;

	return timestamp;
}


BulkInserter::BulkInserter(const std::string& conStr, const std::string& insertDir, int chunkSize, const std::string workerSize, int connPoolSize) : insertDirectory(insertDir), m_chunkSize(chunkSize), workerSize(workerSize)
{
	//LogWarning() << __PRETTY_FUNCTION__ << " 'BULK QUERY CHUNK SIZE: " << m_chunkSize << "'";
	//WARNING_LOG("FeeModule", fmt::format("{} - BULK QUERY CHUNK SIZE:{}", __PRETTY_FUNCTION__, m_chunkSize));
	try
	{
		connectionStringParsed = parseConnectionString(conStr);
		if (dbConnect())
		{
			throw std::exception("DB Connection Failed");
		}
	}
	catch (std::exception& e)
	{
		//LogWarning() << __PRETTY_FUNCTION__ <<  " '" << e.what() << "'";
		//WARNING_LOG("FeeModule", fmt::format("{} - {}", __PRETTY_FUNCTION__, e.what()));
		throw std::exception("DB Connection Failed");
	}

	date = getDate();
	std::filesystem::path dirPath(insertDir + "\\" + date);

	if (!std::filesystem::exists(dirPath))
	{
		if (!std::filesystem::create_directories(dirPath))
		{
			// Directory created successfully
			//LogWarning() << __PRETTY_FUNCTION__ << " '" << "Bulk Directory Creation Failed" << "'";
			// WARNING_LOG("FeeModule", fmt::format("{} - Bulk Directory Creation Failed", __PRETTY_FUNCTION__));
			throw std::exception("Bulk Directory Creation Failed");
		}
	}

	//call function to setMem for all three connections
	std::string query_setmem = "SET work_mem = '" + workerSize+"'";
	setWorkerMemoryForDBConnections(query_setmem);
}

std::vector<std::string> BulkInserter::splitString(const std::string& input, char delimiter)
{
	std::vector<std::string> result;
	std::string token;
	std::stringstream tokenStream(input);
	while (std::getline(tokenStream, token, delimiter)) {
		result.push_back(token);
	}
	return result;
}
std::string BulkInserter::resolvePartitionName(const std::string& tableName, const std::string& date)
{
	// date format: "YYYY-MM-DD"
	// Partition naming: P<TableName>_YYYYMM
	if (date.size() < 8)
		return tableName; // fallback to parent table

	std::string yearMonth = date.substr(0, 4) + date.substr(5, 2);

	// Map parent table names to partition prefixes
	if (tableName == "ExecutionData")
		return "PExecutionData_" + yearMonth;
	else if (tableName == "TradeFeeBreakDown")
		return "PTradeFeeBreakDown_" + yearMonth;
	else if (tableName == "MPIDFeeBreakDown")
		return "PMPIDFeeBreakDown_" + yearMonth;
	else
		return tableName; // no partition mapping, use parent
}
std::string BulkInserter::parseConnectionString(const std::string& connectionString)
{
	// Mapping from ODBC/INI key names to libpq parameter names
	// To support a new parameter, just add a new entry here
	static const std::unordered_map<std::string, std::string> keyMap = {
		{"Database",          "dbname"},
		{"Uid",               "user"},
		{"Pwd",               "password"},
		{"Server",            "host"},
		{"Port",              "port"},
		{"sslmode",           "sslmode"},
		{"KeepaliveTime",     "keepalives_idle"},
		{"KeepaliveInterval", "keepalives_interval"},
		{"KeepaliveCount",    "keepalives_count"},
	};

	// Split the connection string by semicolon (;) to get individual key-value pairs
	std::vector<std::string> pairs = splitString(connectionString, ';');

	std::string convertedString;
	bool hasKeepalive = false;

	// Process each key-value pair
	for (const std::string& pair : pairs) {
		if (pair.empty()) continue;

		// Split each pair by equals (=) to get the key and value
		auto pos = pair.find('=');
		if (pos == std::string::npos) continue;

		std::string key = pair.substr(0, pos);
		std::string value = pair.substr(pos + 1);

		// Look up the libpq equivalent
		auto it = keyMap.find(key);
		if (it != keyMap.end()) {
			if (!convertedString.empty())
				convertedString += " ";
			convertedString += it->second + "=" + value;

			// Track if any keepalive param is present
			if (key.find("Keepalive") != std::string::npos)
				hasKeepalive = true;
		}
	}

	// Auto-enable keepalives if any keepalive parameter was specified
	if (hasKeepalive) {
		convertedString += " keepalives=1";
	}

	return convertedString;
}

int BulkInserter::dbConnect()
{
		PGconn* connection = PQconnectdb(connectionStringParsed.c_str());
		if (PQstatus(connection) == CONNECTION_OK) 
		{
			m_connection = connection;
		} 
		else 
		{
			
			WARNING_LOG("FeeModule", fmt::format("{} - Failed to initialize connection in the pool:{}", __PRETTY_FUNCTION__, PQerrorMessage(connection)));
			PQfinish(connection);
			return 1;
		}
	return 0;
}
int BulkInserter::setWorkerMemoryForDBConnections(const std::string query)
{
		auto conn = m_connection;
		PGresult* res = PQexec(conn, query.c_str());

		if (PQresultStatus(res) != PGRES_COMMAND_OK) {
			WARNING_LOG("FeeModule", fmt::format("{} - SET work_mem failed: {}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
			PQclear(res);
			releaseConnection(conn);
			return 1;
		}
		PQclear(res);

		releaseConnection(conn);
	return 0;	
}
int BulkInserter::fetch_temp_joined_data_pg(const std::string& partial_values,const std::string& date,int offset,QueryResult& result)
{
	result.status = 1;
	result.Data.clear();
	
	PGconn* conn = getConnection();
	if (!conn) {
		WARNING_LOG("FeeModule", "Failed to get DB connection.");
		return 1;
	}

	// 1️⃣ Start a single transaction
	PGresult* res = PQexec(conn, "BEGIN;");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		WARNING_LOG("FeeModule", fmt::format("BEGIN failed: {}", PQerrorMessage(conn)));
		PQclear(res);
		releaseConnection(conn);
		return 1;
	}
	PQclear(res);

	// 2️⃣ Build a single batch SQL query
	std::string sql;

	sql += "CREATE TEMP TABLE temp_order_ids (\"OrderId\" text PRIMARY KEY) ON COMMIT DROP;";
	sql += "INSERT INTO temp_order_ids(\"OrderId\") VALUES " + partial_values + ";";
	sql += "SELECT e.\"OrderId\", e.\"Quantity\", e.\"Price\", e.\"AccountValue\" "
		"FROM \"ExecutionData\" e "
		"JOIN temp_order_ids t ON e.\"OrderId\" = t.\"OrderId\" "
		"WHERE e.\"ExecutionDate\" = '" + date + "' "
		"ORDER BY e.\"OrderId\", e.\"ExecutionId\", e.\"Date\" "
		"LIMIT 100000 OFFSET " + std::to_string(offset) + ";";

	// 3️⃣ Run the full SQL batch
	res = PQexec(conn, sql.c_str());
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		WARNING_LOG("FeeModule", fmt::format("Batch SQL failed: {}", PQerrorMessage(conn)));
		PQclear(res);
		PQexec(conn, "ROLLBACK;");
		releaseConnection(conn);
		return 1;
	}

	// 4️⃣ Extract result rows
	int rows = PQntuples(res);
	int cols = PQnfields(res);

	for (int r = 0; r < rows; ++r)
	{
		std::vector<std::string> row_data;
		row_data.reserve(cols);

		for (int c = 0; c < cols; ++c)
			row_data.push_back(PQgetvalue(res, r, c));

		result.Data.push_back(row_data);
	}

	PQclear(res);

	// 5️⃣ Commit transaction
	res = PQexec(conn, "COMMIT;");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		WARNING_LOG("FeeModule", fmt::format("COMMIT failed: {}", PQerrorMessage(conn)));
		PQclear(res);
		releaseConnection(conn);
		return 1;
	}
	PQclear(res);

	result.status = SQL_SUCCESS;
	releaseConnection(conn);

	return result.status;
}

int BulkInserter::executeQuery(const std::string& query)
{
	int ret = 1;

	auto conn = getConnection();

	PGresult* res = PQexec(conn, query.c_str());
	ExecStatusType status = PQresultStatus(res);

	if (status == PGRES_COMMAND_OK) {
		int affected = atoi(PQcmdTuples(res)); // number of rows affected
		if (affected == 0) {
			INFO_LOG("FeeModule", fmt::format("{} - Successfully Executed But No record found: {}",
				__PRETTY_FUNCTION__, query));
    }
		else {
			INFO_LOG("FeeModule", fmt::format("{} - Successfully Executed {} rows: {}",
				__PRETTY_FUNCTION__, affected, query));
}
		ret = SQL_SUCCESS;
	}
	else {
		WARNING_LOG("FeeModule", fmt::format("{} - Query Failed: {} => {}",
			__PRETTY_FUNCTION__, query, PQerrorMessage(conn)));
	}

	PQclear(res);
	releaseConnection(conn);

	return ret;
}
int BulkInserter::fetchQuery(const std::string& query, QueryResult& result)
{
	result.status = 1; // default to failure
	result.Data.clear();

	auto conn = getConnection();

	PGresult* res = PQexec(conn, query.c_str());
	ExecStatusType status = PQresultStatus(res);

	if (status == PGRES_TUPLES_OK) {
		int rows = PQntuples(res);
		int cols = PQnfields(res);

		for (int r = 0; r < rows; ++r) {
			std::vector<std::string> row_data;
			for (int c = 0; c < cols; ++c) {
				row_data.push_back(PQgetvalue(res, r, c));
			}
			result.Data.push_back(row_data);
		}

		result.status = SQL_SUCCESS;
		PQclear(res);
		releaseConnection(conn);
		return result.status; // stop after first successful connection
	}
	else {
		WARNING_LOG("FeeModule", fmt::format("{} - Query Failed: {} => {}",
			__PRETTY_FUNCTION__, query, PQerrorMessage(conn)));
	}

	PQclear(res);
	releaseConnection(conn);

	return result.status;
}



int BulkInserter::insertFromCSV(std::vector<std::string>& csvDataVector, const std::string& tableName, const std::string& columns)
{

	PGconn* conn = PQconnectdb(connectionStringParsed.c_str());
	if (PQstatus(conn) != CONNECTION_OK) {
		INFO_LOG("FeeModule", fmt::format("{} - Connection failed: {}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
		PQfinish(conn);
		return 1;
	}
	INFO_LOG("FeeModule", fmt::format("{} - Connection Made Successfully", __PRETTY_FUNCTION__));

	if (conn == nullptr)
	{
		WARNING_LOG("FeeModule", fmt::format("{} - Failed to get DB connection", __PRETTY_FUNCTION__));
		return 1;
	}
	// Apply work_mem and statement_timeout to this ad-hoc connection
	{
		std::string setupQuery = "SET work_mem = '" + workerSize + "';";
		PGresult* setupRes = PQexec(conn, setupQuery.c_str());
		if (PQresultStatus(setupRes) != PGRES_COMMAND_OK) {
			WARNING_LOG("FeeModule", fmt::format("{} - Session setup failed: {}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
		}
		PQclear(setupRes);
	}
	int err = 0;
	std::string query = fmt::format("COPY \"{}\" ({}) FROM STDIN WITH (FORMAT csv)", tableName, columns);
	INFO_LOG("FeeModule", fmt::format("{} - Copy Command For Table : {}", __PRETTY_FUNCTION__, tableName));

	// Begin transaction
	PGresult* res = PQexec(conn, "BEGIN");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		WARNING_LOG("FeeModule", fmt::format("{} - BEGIN failed: {}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
		PQclear(res);
		PQfinish(conn);
		return 1;
	}
	INFO_LOG("FeeModule", fmt::format("{} - BEGIN Command successfully Initialised", __PRETTY_FUNCTION__));
	PQclear(res);
	INFO_LOG("FeeModule", fmt::format("{} -BUFFER CLEARED AFTER BEGIN COMMAND", __PRETTY_FUNCTION__));
	// Start COPY 
	res = PQexec(conn, query.c_str());
	INFO_LOG("FeeModule", fmt::format("{} -COPY COMMAND INITIALISED", __PRETTY_FUNCTION__));
	if (PQresultStatus(res) != PGRES_COPY_IN)
	{
		WARNING_LOG("FeeModule", fmt::format("{} - COPY initialization failed: {}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
		PQclear(res);
		PQfinish(conn);
		return 1;
	}
	PQclear(res);
	INFO_LOG("FeeModule", fmt::format("{} -BUFFER CLEAR AFTER SUCCESSFUL COPY INITIALISATION", __PRETTY_FUNCTION__));
	// Stream data in chunks
	std::string queryData;
	queryData.reserve(static_cast<size_t>(m_chunkSize) * 256);
	size_t totalRows = 0;
	size_t totalRecords = csvDataVector.size();

	INFO_LOG("FeeModule", fmt::format("{} - Total Records: {}  CHUNK SIZE: {}", __PRETTY_FUNCTION__, totalRecords, m_chunkSize));
	for (size_t i = 0; i < csvDataVector.size(); ++i)
	{
		queryData += csvDataVector[i] + "\n";
		totalRows++;
		// When chunk size is reached or it's the last record, send it 
		if ((i + 1) % m_chunkSize == 0 || i == csvDataVector.size() - 1)
		{
			int result = PQputCopyData(conn, queryData.c_str(), static_cast<int>(queryData.size()));
			if (result != 1)
			{
				WARNING_LOG("FeeModule", fmt::format("{} - PQputCopyData failed: {}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
				PQputCopyEnd(conn, "Aborted due to client error");
				PQfinish(conn);
				return 1;
			}
			INFO_LOG("FeeModule", fmt::format("{} - COPY DATA SENDING TO BUFFER RESULT: {}", __PRETTY_FUNCTION__, result));
			int flushStatus;
			while ((flushStatus = PQflush(conn)) == 1) {
				std::this_thread::sleep_for(std::chrono::milliseconds(10));
				INFO_LOG("FeeModule", fmt::format("{} - PQFLUSH STATUS INSIDE LOOP: {}", __PRETTY_FUNCTION__, flushStatus));
			}
			if (flushStatus == -1) {
				WARNING_LOG("FeeModule", fmt::format("PQflush failed: {}", PQerrorMessage(conn)));
				PQputCopyEnd(conn, "client flush error");
				INFO_LOG("FeeModule", fmt::format("{} - PQFLUSH STATUS RETURNED -1(I/O) ERROR", __PRETTY_FUNCTION__));
				PQfinish(conn);
				return 1;
			}
			queryData.clear();
			INFO_LOG("FeeModule", fmt::format("{} - COPY SUCCESSFUL FOR ROWS: {}", __PRETTY_FUNCTION__, totalRows));
			// clear chunk buffer 
			INFO_LOG("FeeModule", fmt::format("{} - Added {} records to {}: Remaining Records: {}", __PRETTY_FUNCTION__, totalRows, tableName, totalRecords - totalRows));
		}
	}
	// Signal end of data 
	if (PQputCopyEnd(conn, NULL) != 1)
	{
		WARNING_LOG("FeeModule", fmt::format("{} - COPY COMMAND failed: {}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
		PQfinish(conn);
		return 1;
	}
	INFO_LOG("FeeModule", fmt::format("{} - COPY END SIGNAL SUCCESSFUL", __PRETTY_FUNCTION__));
	// Check for COPY result — drain ALL results before acting on error
	bool copyFailed = false;
	while ((res = PQgetResult(conn)) != NULL)
	{
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			WARNING_LOG("FeeModule", fmt::format("{} - COPY failed: {}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
			copyFailed = true;
		}
		PQclear(res);
	}
	if (copyFailed)
	{
		PQfinish(conn);
		return 1;
	}
	INFO_LOG("FeeModule", fmt::format("{} - COPY SUCCESSFUL", __PRETTY_FUNCTION__));
	res = PQexec(conn, "COMMIT");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		WARNING_LOG("FeeModule", fmt::format("{} - COMMIT failed: {}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
		PQclear(res);
		PQfinish(conn);
		return 1;
	}
	INFO_LOG("FeeModule", fmt::format("{} - SUCCESSFUL COMMIT", __PRETTY_FUNCTION__));
	PQclear(res);
	INFO_LOG("FeeModule", fmt::format("{} - Successfully inserted {} records into {}", __PRETTY_FUNCTION__, totalRows, tableName));
	csvDataVector.clear();
	PQfinish(conn);
	return 0;
}

UpdateClause BulkInserter::generateUpdateSetWithWhere(const std::string& tableName,const std::string& columns, const std::string& uniqueColsQuoted)
{
	UpdateClause result;
	std::vector<std::string> setParts;
	std::vector<std::string> whereParts;

	// Convert uniqueColumns into a fast lookup set
	std::unordered_set<std::string> uniqueSet;
	{
		std::stringstream ss(uniqueColsQuoted);
		std::string tok;
		while (std::getline(ss, tok, ',')) {
			tok.erase(0, tok.find_first_not_of(" \t\""));
			tok.erase(tok.find_last_not_of(" \t\"") + 1);
			uniqueSet.insert(tok);
		}
	}

	// Parse columns list
	std::stringstream ss(columns);
	std::string token;
	while (std::getline(ss, token, ',')) {

		token.erase(0, token.find_first_not_of(" \t\""));
		token.erase(token.find_last_not_of(" \t\"") + 1);

		// Skip unique key columns
		if (uniqueSet.count(token)) continue;

		// Add to SET clause ("Col" = EXCLUDED."Col")
		setParts.push_back(
			fmt::format("\"{0}\" = EXCLUDED.\"{0}\"", token)
		);

		// Add to WHERE clause (ONLY updates if value changed)
		whereParts.push_back(
			fmt::format("\"{0}\".\"{1}\" IS DISTINCT FROM EXCLUDED.\"{1}\"",
				tableName, token)
		);
	}

	//result.setClause = join(setParts, ", ");
	//result.whereClause = join(whereParts, " OR ");

	return result;
}


int BulkInserter::upsertFromCSV(std::vector<std::string>& csvDataVector,const std::string& tableName,const std::string& columns,const std::string& uniqueColsQuoted,size_t batchSize) 
{
	PGconn* conn = PQconnectdb(connectionStringParsed.c_str());
	if (PQstatus(conn) != CONNECTION_OK) {
		INFO_LOG("FeeModule", fmt::format("{} - Connection failed: {}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
		PQfinish(conn);
		return 1;
	}

	PGresult* res = PQexec(conn, "BEGIN");
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		WARNING_LOG("FeeModule", fmt::format("{} - BEGIN failed: {}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
		PQclear(res);
		PQfinish(conn);
		return 1;
	}
	PQclear(res);

	// 1) Create temp staging table
	std::string createTemp = fmt::format(
		"CREATE TEMP TABLE staging_{0} (LIKE \"{0}\" INCLUDING ALL) ON COMMIT DROP;",
		tableName
	);
	res = PQexec(conn, createTemp.c_str());
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		WARNING_LOG("FeeModule", fmt::format("{} - CREATE TEMP failed: {}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
		PQclear(res);
		PQexec(conn, "ROLLBACK");
		PQfinish(conn);
		return 1;
	}
	PQclear(res);

	// 2) COPY into staging
	std::string copyCmd = fmt::format("COPY staging_{0} ({1}) FROM STDIN WITH (FORMAT csv)", tableName, columns);
	res = PQexec(conn, copyCmd.c_str());
	if (PQresultStatus(res) != PGRES_COPY_IN) {
		WARNING_LOG("FeeModule", fmt::format("{} - COPY init failed: {}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
		PQclear(res);
		PQexec(conn, "ROLLBACK");
		PQfinish(conn);
		return 1;
	}
	PQclear(res);

	// Stream CSV rows in chunks
	std::string chunk;
	for (size_t i = 0; i < csvDataVector.size(); ++i) {
		chunk += csvDataVector[i] + "\n";
		if ((i + 1) % m_chunkSize == 0 || i == csvDataVector.size() - 1) {
			if (PQputCopyData(conn, chunk.c_str(), static_cast<int>(chunk.size())) != 1) {
				WARNING_LOG("FeeModule", fmt::format("{} - PQputCopyData failed: {}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
				PQputCopyEnd(conn, "client error");
				PQexec(conn, "ROLLBACK");
				PQfinish(conn);
				return 1;
			}
			int flushStatus;
			while ((flushStatus = PQflush(conn)) == 1)
				std::this_thread::sleep_for(std::chrono::milliseconds(10));
			if (flushStatus == -1) {
				WARNING_LOG("FeeModule", fmt::format("{} - PQflush I/O failed: {}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
				PQputCopyEnd(conn, "client flush error");
				PQexec(conn, "ROLLBACK");
				PQfinish(conn);
				return 1;
			}
			chunk.clear();
		}
	}

	if (PQputCopyEnd(conn, NULL) != 1) {
		WARNING_LOG("FeeModule", fmt::format("{} - PQputCopyEnd failed: {}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
		PQexec(conn, "ROLLBACK");
		PQfinish(conn);
		return 1;
	}

	while ((res = PQgetResult(conn)) != NULL) {
		if (PQresultStatus(res) != PGRES_COMMAND_OK) {
			WARNING_LOG("FeeModule", fmt::format("{} - COPY error: {}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
			PQclear(res);
			PQexec(conn, "ROLLBACK");
			PQfinish(conn);
			return 1;
		}
		PQclear(res);
	}

	// 3) UPSERT in batches
	UpdateClause uc = generateUpdateSetWithWhere(tableName,columns, uniqueColsQuoted);

	// Count rows in staging
	res = PQexec(conn, fmt::format("SELECT COUNT(*) FROM staging_{}", tableName).c_str());
	if (PQresultStatus(res) != PGRES_TUPLES_OK) {
		WARNING_LOG("FeeModule", fmt::format("{} - COUNT staging failed: {}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
		PQclear(res);
		PQexec(conn, "ROLLBACK");
		PQfinish(conn);
		return 1;
	}
	long totalRows = std::stol(PQgetvalue(res, 0, 0));
	PQclear(res);

	for (long offset = 0; offset < totalRows; offset += batchSize) {
		std::string batchSql = fmt::format(
			"INSERT INTO \"{0}\" ({1}) "
			"SELECT {1} FROM staging_{0} "
			"ORDER BY ctid LIMIT {2} OFFSET {3} "
			"ON CONFLICT ({4}) DO UPDATE SET {5} "
			"WHERE {6};",
			tableName,
			columns,
			batchSize,
			offset,
			uniqueColsQuoted,
			uc.setClause,
			uc.whereClause
		);

		res = PQexec(conn, batchSql.c_str());
		if (PQresultStatus(res) != PGRES_COMMAND_OK) {
			WARNING_LOG("FeeModule", fmt::format("{} - UPSERT batch failed: {}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
			PQclear(res);
			PQexec(conn, "ROLLBACK");
			PQfinish(conn);
			return 1;
		}

		PQclear(res);
	}

	// 4) Commit transaction
	res = PQexec(conn, "COMMIT");
	if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		WARNING_LOG("FeeModule", fmt::format("{} - COMMIT failed: {}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
		PQclear(res);
		PQexec(conn, "ROLLBACK");
		PQfinish(conn);
		return 1;
	}
	PQclear(res);

	csvDataVector.clear();
	PQfinish(conn);
	return 0;
}


void BulkInserter::releaseConnection(PGconn* connection) 
{

}

PGconn* BulkInserter::getConnection() 
{
	return m_connection;
}

int BulkInserter::prepareQuery(const std::string& query)
{
	// Start a transaction
	auto conn = getConnection();
	if (nullptr == conn) return 1;

	PGresult* beginTransactionResult = PQexec(conn, "BEGIN");
	if (PQresultStatus(beginTransactionResult) != PGRES_COMMAND_OK) {
		//LogWarning() << "BEGIN command failed: " << PQerrorMessage(conn) << std::endl;
		WARNING_LOG("FeeModule", fmt::format("{} - BEGIN command failed:{}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
		PQclear(beginTransactionResult);
			releaseConnection(conn);
		return 1;
	}

	// Start the COPY command
	PGresult* copyResult = PQexec(conn, query.c_str());
	if (PQresultStatus(copyResult) != PGRES_COPY_IN)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'COPY command initialization failed: " << PQerrorMessage(conn) << "'";
		WARNING_LOG("FeeModule", fmt::format("{} - COPY command initialization failed:{}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
		PQclear(copyResult);
		releaseConnection(conn);
		return 1;
	}
	//PQclear(commit);

	releaseConnection(conn);
	return 0;
}

void BulkInserter::writeFile(const std::string& filename, const std::string& content, const std::string& headers)
{
	std::ofstream file(insertDirectory + "/" + date + "/" + filename + ".csv");
	if (file.fail())
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Unable to write to Bulk Insertion File: " << filename << "'";
		WARNING_LOG("FeeModule", fmt::format("{} - Unable to write to Bulk Insertion File:{}", __PRETTY_FUNCTION__, filename));
		return;
	}

	file << headers << "\n" << content;
	//LogInfo() << __PRETTY_FUNCTION__ << " 'Successfully wrote to Bulk Insertion File: " << filename << "'";
	INFO_LOG("FeeModule", fmt::format("{} - Successfully wrote to Bulk Insertion File: {}", __PRETTY_FUNCTION__, filename));
}

int BulkInserter::executeCopyStmt(std::string &queryData)
{
	auto conn = getConnection();
	if (nullptr == conn) return 1;

	int dataLength = static_cast<int> (queryData.length());
	int result = PQputCopyData(conn, queryData.c_str(), dataLength);
	if (result != 1)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Failed to write data to COPY command: " << PQerrorMessage(conn) << "'";
		WARNING_LOG("FeeModule", fmt::format("{} - Failed to write data to COPY command:{}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
		releaseConnection(conn);
		return 1;
	}


	// Signal the end of data
	result = PQputCopyEnd(conn, NULL);
	if (result != 1)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Failed to signal the end of data: " << PQerrorMessage(conn) << "'";
		WARNING_LOG("FeeModule", fmt::format("{} - Failed to signal the end of data: {}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
		releaseConnection(conn);
		return 1;
	}

	// Check for any errors during the COPY operation
	PGresult* copyResult;
	while ((copyResult = PQgetResult(conn)) != NULL)
	{
		ExecStatusType copyStatus = PQresultStatus(copyResult);
		if (copyStatus != PGRES_COMMAND_OK)
		{
			//LogWarning() << __PRETTY_FUNCTION__ << " 'COPY command failed: " << PQerrorMessage(conn) << "'" << "'";
			WARNING_LOG("FeeModule", fmt::format("{} - COPY command failed:{}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
			PQclear(copyResult);
			releaseConnection(conn);
			return 1;
		}
		PQclear(copyResult);
	}

	// Commit the transaction
	PGresult* commitResult = PQexec(conn, "COMMIT");
	if (PQresultStatus(commitResult) != PGRES_COMMAND_OK)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << "COMMIT command failed: " << PQerrorMessage(conn) << std::endl;
		WARNING_LOG("FeeModule", fmt::format("{} - COMMIT command failed:{}", __PRETTY_FUNCTION__, PQerrorMessage(conn)));
		releaseConnection(conn);
		return 1;
	}
	PQclear(commitResult);

	releaseConnection(conn);
	return 0;
}


int BulkInserter::RetrieveData(const std::string& query, std::vector<std::vector<std::string>>& ret)
{
	auto conn = getConnection();
	if (nullptr == conn) return 1;

	// Execute the query
	PGresult* result = PQexec(conn, query.c_str());
	if (PQresultStatus(result) != PGRES_TUPLES_OK) {
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Query execution failed: " << PQresultErrorMessage(result) << std::endl;
		WARNING_LOG("FeeModule", fmt::format("{} - Query execution failed:{}", __PRETTY_FUNCTION__, PQresultErrorMessage(result)));
		PQclear(result);
		releaseConnection(conn);
		return 1;
	}

	// Retrieve data from the result set
	int numRows = PQntuples(result);
	int numCols = PQnfields(result);

	for (int row = 0; row < numRows; ++row) {
		for (int col = 0; col < numCols; ++col)
		{
			const char* value = PQgetvalue(result, row, col);
			if (value != nullptr) {
				ret[row][col] = value;
			}
			else {
				// Handle NULL value, such as assigning an empty string
				ret[row][col] = "";
			}
			value = PQgetvalue(result, row, col);
			ret[row][col] = value;
			//std::cout << "Row: " << row << ", Col: " << col << ", Value: " << value << std::endl;
		}
	}

	// Cleanup
	PQclear(result);
	releaseConnection(conn);
	return 0;

}


void BulkInserter::cancelAndCloseConnection(PGconn*& conn)
{
	if (!conn)
		return;
	try
	{
		if (PQstatus(conn) == CONNECTION_OK)
		{
			PGcancel* cancel = PQgetCancel(conn);
			if (cancel)
			{
				char errbuf[256] = { 0 };
				PQcancel(cancel, errbuf, sizeof(errbuf));
				PQfreeCancel(cancel);
			}
		}
		PQfinish(conn);
	}
	catch (const std::exception& e)
	{
		WARNING_LOG("FeeModule", fmt::format("{} - Exception while closing a connection: {}", __PRETTY_FUNCTION__, e.what()));
	}
	conn = nullptr;
}
BulkInserter::~BulkInserter()
{
	cancelAndCloseConnection(m_connection);

}
