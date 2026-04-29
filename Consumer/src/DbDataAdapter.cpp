#include "../FeeModule/DbDataAdapter.h"
#include "../FeeModule/logger/logger.hpp"
#include <boost/algorithm/string/erase.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
//#include <fmt/format.h>
#include <iomanip>
#include "../FeeModule/rapidjson/document.h"
#define CHECK_NULL(str)  (str.empty() ? "null" : fmt::format("'{}'", str))

DbDataAdapter* DbDataAdapter::instance = nullptr; //Static Variable Initialization



DbDataAdapter::DbDataAdapter(const std::string conn_static, const std::string conn_transact,
	const std::string log_path_static, const std::string log_path_transact,
	const bool run_queue, const std::string date, const int bulkChunkSize, const unsigned int query_time_out, const std::string log_path_stack, const std::string worker_size,
	const std::string server_ip, const short port_num, const bool run_messenger):
	todayDate(date), //Defined for both the types of FCM
#ifdef BULK_INSERT
	//databaseManager_static(conn_static, log_path_static, false, query_time_out, log_path_stack),
	//databaseManager_dynamic(conn_transact, log_path_transact, run_queue, query_time_out, log_path_stack),
	db_inserter(conn_transact, log_path_transact, bulkChunkSize, worker_size),
	isRealTime(run_queue),
	m_connStringStatic(conn_static)
{ }
#else
	//databaseManager_static(conn_static, log_path_static, false, query_time_out, log_path_stack),
	//databaseManager_dynamic(conn_transact, log_path_transact, run_queue, query_time_out, log_path_stack, server_ip, port_num, run_messenger),
	//m_connStringStatic(conn_static)
	//Provider Server Ip & Port And Run Messesnger
	db_inserter(conn_transact, log_path_transact, bulkChunkSize, worker_size),
	isRealTime(run_queue),
	m_connStringStatic(conn_static)
{ }
#endif

DbDataAdapter* DbDataAdapter::getInstance(LSL::FeeModule::IniParser configs, const std::string date)
{
	if (!DbDataAdapter::instance)
	{
		try
		{
			bool isSyncEnabled = configs.Get<short>("SyncServer.IsEnabled");
			if (isSyncEnabled)
			{
				DbDataAdapter::instance = new DbDataAdapter(configs.Get<std::string>("Database.ConnectionStringStatic"),
					configs.Get<std::string>("Database.ConnectionStringTransact"),
					configs.Get<std::string>("Database.LogPathStatic"),
					configs.Get<std::string>("Database.LogPathTransact"),
					configs.Get<short>("Database.RunQueue"),
					date,
					0, //Chunk Size is 0 For Static DB
					configs.Get<short>("Database.QueryTimeOut"),
					configs.Get<std::string>("Logging.Path"),
					configs.Get<std::string>("Database.workerSize", "4MB"),
					configs.Get<std::string>("SyncServer.ServerIP"),
					configs.Get<int>("SyncServer.ServerPort"),
					true
				);
			}
			else
			{
				DbDataAdapter::instance = new DbDataAdapter(configs.Get<std::string>("Database.ConnectionStringStatic"),
					configs.Get<std::string>("Database.ConnectionStringTransact"),
					configs.Get<std::string>("Database.LogPathStatic"),
					configs.Get<std::string>("Database.LogPathTransact"),
					configs.Get<short>("Database.RunQueue"),
					date,
					configs.Get<int>("Database.ChunkSize"),
					configs.Get<int>("Database.QueryTimeOut"),
					configs.Get<std::string>("Logging.Path"),
					configs.Get<std::string>("Database.workerSize","4MB")
				);
			}

		}
		catch (std::exception e)
		{
		
			std::cerr << "Exception Caught: " << e.what();
			std::abort();
		} 
	}
	return DbDataAdapter::instance;
}

DbDataAdapter* DbDataAdapter::getInstance()
{
	if (DbDataAdapter::instance)
	{
		return DbDataAdapter::instance;
	}

	//WARNING_LOG("FeeModule", fmt::format("{} - called without Initializing", __PRETTY_FUNCTION__));
	std::abort();
}

int DbDataAdapter::fetchDataFromStaticDb(const std::string& query, QueryResult& result)
{
	////WARNING_LOG("FeeModule", fmt::format("{} - Fetching Data From static DB", __PRETTY_FUNCTION__));
	return db_inserter.fetchQuery(query, result);
}
int DbDataAdapter::ExecuteQuery(const std::string& query)
{

	//WARNING_LOG("FeeModule", fmt::format("{} - Execute Query In From static DB", __PRETTY_FUNCTION__));
	return db_inserter.executeQuery(query);
}
void DbDataAdapter::disconnectStaticDb()
{
	//WARNING_LOG("FeeModule", fmt::format("{} - DISCONNECTING STATIC DB", __PRETTY_FUNCTION__));
	}

int DbDataAdapter::dataRetrievedPlans(std::vector <Plan>& plans)
{
	std::string query = "SELECT \"Id\", \"Name\", \"FeeCategoryId\", \"RoundingPoints\", \"PlanType\", \"Rounding\", \"FirmId\", \"FeeCategoryType\", \"ParentPlanId\" FROM \"Plans\" WHERE \"IsActive\" = true;\n\0";
	QueryResult result;
	int err = fetchDataFromStaticDb(query, result);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		return 1;
	}
	else
	{
		for (int i = 0; i < result.Data.size(); i++)
		{
			Plan p1;
			p1.id = result.Data[i][0];
			p1.name = result.Data[i][1];
			p1.feeCategoryId = result.Data[i][2];
			p1.roundingPoints = std::stoi(result.Data[i][3]);
			switch (std::stoi(result.Data[i][4]))
			{
			case EN_PlanType_None:
				p1.planType = EN_PlanType_None;
				break;
			case EN_PlanType_PerOrder:
				p1.planType = EN_PlanType_PerOrder;
				break;
			case EN_PlanType_PerExecution:
				p1.planType = EN_PlanType_PerExecution;
				break;
			}
			switch (std::stoi(result.Data[i][5]))
			{
			case EN_RoundingScheme_HalfUp:
				p1.rounding = EN_RoundingScheme_HalfUp;
				break;
			case EN_RoundingScheme_HalfDown:
				p1.rounding = EN_RoundingScheme_HalfDown;
				break;
			case EN_RoundingScheme_AlwaysUp:
				p1.rounding = EN_RoundingScheme_AlwaysUp;
				break;
			case EN_RoundingScheme_AlwaysDown:
				p1.rounding = EN_RoundingScheme_AlwaysDown;
				break;
			case EN_RoundingScheme_Truncate:
				p1.rounding = EN_RoundingScheme_Truncate;
				break;
			case EN_RoundingScheme_Count:
				p1.rounding = EN_RoundingScheme_Count;
				break;
			}
			p1.firmId = result.Data[i][6];
			//New Data Retrieved

			switch (std::stoi(result.Data[i][7]))
			{
			case EN_FeeCategoryType_TradeFee:
				p1.feeTypeId = EN_FeeCategoryType_TradeFee;
				break;
			case EN_FeeCategoryType_MpidFee:
				p1.feeTypeId = EN_FeeCategoryType_MpidFee;
				break;
			default:
				p1.feeTypeId = EN_FeeCategoryType_None;
			}

			p1.parentPlanId = result.Data[i][8];

			plans.emplace_back(p1);
		}
	}
	//DEBUG_LOG("FeeModule", fmt::format("{} - Plans Retrieved From Database", __PRETTY_FUNCTION__));
	return 0;
}

int DbDataAdapter::dataRetrievedTradeFeeCategories(std::vector <FeeType>& feeType)
{
	std::string query = "SELECT \"Id\",\"Name\" FROM \"FeeCategory\" WHERE \"IsActive\" = true;\n\0";
	QueryResult result;
	int err = fetchDataFromStaticDb(query, result);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		return 1;
	}
	else
	{
		for (int i = 0; i < result.Data.size(); i++)
		{
			FeeType f1;
			f1.id = result.Data[i][0];
			f1.name = result.Data[i][1];
			feeType.emplace_back(f1);
		}
	}
	//DEBUG_LOG("FeeModule", fmt::format("{} - Fee Categories Retrieved From Database", __PRETTY_FUNCTION__));

	return 0;
}

int DbDataAdapter::dataRetrievedFormula(FormulaData& formulaData, const std::string& formulaID)
{
	std::string typesNeeded = "(" + std::to_string(EN_FeeCategoryType_TradeFee) +
		"," + std::to_string(EN_FeeCategoryType_MpidFee) + ")";

	std::string query = "SELECT \"Id\", \"FormulaType\", \"Formula\", \"Name\" FROM \"Plans\" WHERE \"Id\" = '" + formulaID +
		"' AND \"FeeCategoryType\" in " + typesNeeded + " AND \"IsActive\" = true; \n\0";

	QueryResult result;
	int err = fetchDataFromStaticDb(query, result);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		return 1;
	}
	if (result.Data.size() <= 0)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - No Formulas To Retrieve From Database with conditions for Plan ID: {}", __PRETTY_FUNCTION__, formulaID));
	}
	else
	{
		formulaData.planId = result.Data[0][0];
		switch (std::stoi(result.Data[0][1]))
		{
		case EN_FormulaType_Tabular:
			formulaData.formulaType = EN_FormulaType_Tabular;
			break;
		case EN_FormulaType_Tiered:
			formulaData.formulaType = EN_FormulaType_Tiered;
			break;
		case EN_FormulaType_Raw:
			formulaData.formulaType = EN_FormulaType_Raw;
			break;
		case EN_FormulaType_Builder:
			formulaData.formulaType = EN_FormulaType_Builder;
			break;
		case EN_FormulaType_AdvBuilder:
			formulaData.formulaType = EN_FormulaType_AdvBuilder;
			break;
		case EN_FormulaType_Coded:
			formulaData.formulaType = EN_FormulaType_Coded;
			break;
		default:
			formulaData.formulaType = EN_FormulaType_None;
			//WARNING_LOG("FeeModule", fmt::format("{} - Formula Type is None for PlanId: {}", __PRETTY_FUNCTION__, formulaData.planId));
		}

		formulaData.formulaJSON = result.Data[0][2];
		formulaData.formulaName = result.Data[0][3];

		//DEBUG_LOG("FeeModule", fmt::format("{} - Formulas Retrieved From Database", __PRETTY_FUNCTION__));
	}
	return err;
}

int DbDataAdapter::dataRetrievedFormulaId(std::vector<std::vector<std::string>>& ret)
{
	std::string typesNeeded = "(" + std::to_string(EN_FeeCategoryType_TradeFee) +
		"," + std::to_string(EN_FeeCategoryType_MpidFee) + ")";

	std::string query = "SELECT \"Id\" FROM \"Plans\" WHERE \"FeeCategoryType\" in " + typesNeeded + " AND \"IsActive\" = True;\n";
	QueryResult result;
	int err = fetchDataFromStaticDb(query, result);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		return 1;
	}
	ret = result.Data;

	//DEBUG_LOG("FeeModule", fmt::format("{} - Only IDs of Plans Retrieved From Database", __PRETTY_FUNCTION__));
	return err;
}

int DbDataAdapter::dataRetrievedTradeFeePlan(std::vector<TradeFee>& tradeFees)
{
	std::string typesNeeded = "(" + std::to_string(EN_FeeCategoryType_TradeFee) +
		"," + std::to_string(EN_FeeCategoryType_MpidFee) + ")";

	std::string select = "SELECT T.\"Id\", T.\"PlanId\", T.\"AccessId\", T.\"AccessType\" FROM \"TradeFees\" T LEFT JOIN \"TradeFeeDetails\" D on T.\"Id\" = D.\"TradeFeeId\" ";
	std::string whereCheck = "WHERE \"IsActive\" = true AND \"FeeCategoryType\" in " + typesNeeded + " AND D.\"IsNoPlan\" = false AND ";
	std::string dateCheck = "CAST(D.\"StartDate\" as Date) <= '" + this->todayDate + "' and (CAST(D.\"EndDate\" as Date) >= '" + this->todayDate + "' or D.\"EndDate\" is null);\n\0";
	std::string query = select + whereCheck + dateCheck;

	QueryResult result;
	int err = fetchDataFromStaticDb(query, result);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		return 1;
	}
	else
	{
		for (int i = 0; i < result.Data.size(); i++)
		{
			TradeFee t1;
			t1.id = result.Data[i][0];
			t1.planId = result.Data[i][1];
			t1.accessId = result.Data[i][2];

			switch (std::stoi(result.Data[i][3]))
			{
			case EN_AccessType_Account:
				t1.accessType = EN_AccessType_Account;
				t1.accountId = t1.accessId;
				break;
			case EN_AccessType_Firm:
				t1.accessType = EN_AccessType_Firm;
				t1.firmId = t1.accessId;
				break;
			case EN_AccessType_MPID:
				t1.accessType = EN_AccessType_MPID;
				t1.mpidId = t1.accessId;
				break;
			default:
				t1.accessType = EN_AccessType_None;
				//WARNING_LOG("FeeModule", fmt::format("{} - Access Type is None for TradeFeesId: {}", __PRETTY_FUNCTION__, t1.id));
			}
			tradeFees.emplace_back(t1);
		}
	}
	//DEBUG_LOG("FeeModule", fmt::format("{} - Trade Fees Retrieved From Database", __PRETTY_FUNCTION__));
	return 0;
}

int DbDataAdapter::dataRetrievedAccounts(std::vector<Account>& accounts)
{
	std::string query = "SELECT A.\"Id\", A.\"Value\", A.\"FirmId\", A.\"MPIDId\", MP.\"Value\" FROM \"Accounts\" A JOIN \"MPID\" MP ON MP.\"Id\" = A.\"MPIDId\" WHERE A.\"IsActive\" = true;\n\0";

	QueryResult result;
	int err = fetchDataFromStaticDb(query, result);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		return 1;
	}
	else
	{
		for (int i = 0; i < result.Data.size(); i++)
		{
			Account a1;
			a1.id = result.Data[i][0];
			boost::algorithm::to_upper(result.Data[i][1]);
			a1.name = result.Data[i][1];
			a1.firmId = result.Data[i][2];
			a1.mpidId = result.Data[i][3];
			a1.mpidValue = result.Data[i][4];
			accounts.emplace_back(a1);
		}
	}
	
	//DEBUG_LOG("FeeModule", fmt::format("{} - Accounts & their Groups Retrieved From Database", __PRETTY_FUNCTION__));
	return 0;
}
//This function is to handle setWorker Memory for Postgres Database
int DbDataAdapter::setWorkerMemory(const std::string& WorkerSize)
{
	std::string query_setmem = "SET work_mem = '" + WorkerSize + "'";

	int err = ExecuteQuery(query_setmem);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		
	}
	else
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Success", __PRETTY_FUNCTION__));
	}
	
	return err;
}
int DbDataAdapter::dataRetrievedOrderExecution(std::vector<OrderExecutionData>& orderExecutionData)
{
	std::int64_t offset = 0;
	std::int64_t reserve = 0;
	const int limit = 50000;
	QueryResult res;
	std::string getRows = "SELECT COUNT(*) AS row_count FROM \"ExecutionData\";";
	//int err = databaseManager_dynamic.fetch_data(getRows, res);
	int err = db_inserter.fetchQuery(getRows, res);
	if (err)
		return 0;
	else
		reserve = std::stoll(res.Data[0][0]);

	orderExecutionData.reserve(reserve);

	while (true)
	{
		//Hitting Transact DB Only For All Data
		const char* temp = "SELECT [OrderId],[AccountValue],[Date],[LastShares],[Type],[MPIDRecv],[FirmId] FROM [ExecutionData] "
			"ORDER BY [OrderId],[ExecutionId],[Date] OFFSET ";
		std::string query = temp + std::to_string(offset) + " ROWS FETCH NEXT " + std::to_string(limit) + " ROWS ONLY;\n\0";
		offset += limit;


		offset += 50000;
		QueryResult result;
		int err = db_inserter.fetchQuery(query, result);
		//int err = databaseManager_dynamic.fetch_data(query, result);
		if (err == 1)
		{
			//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
			//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
			return 1;
		}

		else
		{
			for (const auto& data : result.Data)
			{
				OrderExecutionData execution;
				execution.orderId = data[0];
				execution.accountValue = data[1];
				boost::algorithm::to_upper(execution.accountValue);
				execution.date = data[2];
				execution.lastShares = std::stod(data[3]);							//Adding these to add support for recovery
				execution.type = static_cast<InstrumentType>(std::stoi(data[4]));	//for real-time system
				execution.mpidRecv = data[5];
				execution.firmId = data[6];
				orderExecutionData.emplace_back(execution);
			}
		}
		if (result.Data.size() < limit)
		{
			break;
		}
		//DEBUG_LOG("FeeModule", fmt::format("{} - Executions Data Retrieved From Database", __PRETTY_FUNCTION__));
	}

	return 0;
}

int DbDataAdapter::dataRetrievedSymbolData(std::vector<std::vector<std::string>>& data)
{
	std::string query = "SELECT DISTINCT ON (\"Symbol\") \"Symbol\", \"IsTestSymbol\", \"Tape\", \"Date\" FROM \"SecurityMasters\" ORDER BY \"Symbol\", \"Date\" DESC;";

	QueryResult result;
	int err = fetchDataFromStaticDb(query, result);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		return 1;
	}
	data = result.Data;
	//DEBUG_LOG("FeeModule", fmt::format("{} - Symbols Data If Available were Retrieved From Database", __PRETTY_FUNCTION__));
	return err;
}

int DbDataAdapter::dataRetrievedMonthlyDetails(std::vector<MonthlyDetails>& monthlyDetailsData, const std::string& date)
{
	std::string query{ "" };

	if (date.empty())
	{
		query = "SELECT \"AccountValue\", \"MonthlyVolume\", \"TradeType\" FROM \"MonthlyDetails\" Where CAST(\"Date\" AS Date) = (SELECT CAST(MAX(\"Date\") AS Date) From \"MonthlyDetails\");";
	}
	else
	{
		std::string wherePart = " WHERE \"Date\" < '" + date + "');";
		query = "SELECT \"AccountValue\", \"MonthlyVolume\", \"TradeType\" FROM \"MonthlyDetails\" Where CAST(\"Date\" AS Date) = (SELECT CAST(MAX(\"Date\") AS Date) From \"MonthlyDetails\"" + wherePart;
	}

	QueryResult result;
	int err = fetchDataFromStaticDb(query, result);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		return 1;
	}
	else
	{
		for (const auto& data : result.Data)
		{
			MonthlyDetails details;
			details.accountValue = data[0];
			details.monthlyVolume = std::stod(data[1]);
			details.tradeType = std::stoi(data[2]);
			monthlyDetailsData.emplace_back(details);
		}
	}
	//DEBUG_LOG("FeeModule", fmt::format("{} - Monthly Details Retrieved From Database", __PRETTY_FUNCTION__));
	return 0;
}

int DbDataAdapter::dataRetrievedMPIDMonthlyDetails(std::vector<MPIDMonthlyDetails>& mpidMonthlyDetailsData, const std::string& date)
{
	std::string query{ "" };

	if (date.empty())
	{
		query = "SELECT \"MPIDId\", \"MonthlyVolume\", \"TradeType\" FROM \"MPIDMonthlyDetails\" Where CAST(\"Date\" AS Date) = (SELECT CAST(MAX(\"Date\") AS Date) From \"MPIDMonthlyDetails\");";
	}
	else
	{
		std::string wherePart = " WHERE \"Date\" < '" + date + "');";
		query = "SELECT \"MPIDId\", \"MonthlyVolume\", \"TradeType\" FROM \"MPIDMonthlyDetails\" Where CAST(\"Date\" AS Date) = (SELECT CAST(MAX(\"Date\") AS Date) From \"MPIDMonthlyDetails\"" + wherePart;
	}

	QueryResult result;
	int err = fetchDataFromStaticDb(query, result);
	if (err == 1)
	{
		LogWarning() << __PRETTY_FUNCTION__ << " 'DB Query Failed'";
		LogWarning() << "Query: " << query;
		return 1;
	}
	else
	{
		for (const auto& data : result.Data)
		{
			MPIDMonthlyDetails details;
			details.mpidId = data[0];
			details.monthlyVolume = std::stoll(data[1]);
			details.tradeType = std::stoi(data[2]);
			mpidMonthlyDetailsData.emplace_back(details);
		}
	}
	LogDebug() << __PRETTY_FUNCTION__ << " 'Monthly Details Retrieved From Database'";
	return 0;
}

int DbDataAdapter::dataRetrievedFirmMonthlyDetails(std::vector<FirmMonthlyDetails>& firmMonthlyDetailsData, const std::string& date)
{
	std::string query{ "" };

	if (date.empty())
	{
		query = "SELECT \"FirmId\", \"MonthlyVolume\", \"TradeType\" FROM \"FirmMonthlyDetails\" Where CAST(\"Date\" AS Date) = (SELECT CAST(MAX(\"Date\") AS Date) From \"FirmMonthlyDetails\");";
	}
	else
	{
		std::string wherePart = " WHERE \"Date\" < '" + date + "');";
		query = "SELECT \"FirmId\", \"MonthlyVolume\", \"TradeType\" FROM \"FirmMonthlyDetails\" Where CAST(\"Date\" AS Date) = (SELECT CAST(MAX(\"Date\") AS Date) From \"FirmMonthlyDetails\"" + wherePart;
	}

	QueryResult result;
	int err = fetchDataFromStaticDb(query, result);
	if (err == 1)
	{
		LogWarning() << __PRETTY_FUNCTION__ << " 'DB Query Failed'";
		LogWarning() << "Query: " << query;
		return 1;
	}
	else
	{
		for (const auto& data : result.Data)
		{
			FirmMonthlyDetails details;
			details.firmId = data[0];
			details.monthlyVolume = std::stoll(data[1]);
			details.tradeType = std::stoi(data[2]);
			firmMonthlyDetailsData.emplace_back(details);
		}
	}
	LogDebug() << __PRETTY_FUNCTION__ << " 'Monthly Details Retrieved From Database'";
	return 0;
}


void DbDataAdapter::filldataFeeCommissionsStore(const OrderExecutionData& data, const FeeComissionData& feeCommission, const FeeComissionData& mpidFee)
{

#ifdef BULK_INSERT

	timeStamp = getTimeStamp();
	model.ExecutionData.push_back(fillExecutionDataTable(data));
	{
		auto breakdowns = fillTradeFeeBreakDownTable(data, feeCommission);
		for (auto breakdown : breakdowns)
		{
			model.TradeFeeBreakDownData.emplace_back(breakdown);
		}
	}
	{
		auto breakdowns = fillMPIDBreakDownTable(data, mpidFee);
		for (auto breakdown : breakdowns)
		{
			model.MPIDFeeBreakDownData.emplace_back(breakdown);
		}
	}

#else

	//fillExecutionDataTable(data);
	//fillTradeFeesAndCommissionsTable(data, feeCommission);
	//fillMPIDFeesAndCommissionsTable(data, mpidFee);
	//fillTradeFeeBreakDownTable(data, feeCommission);
	//fillMPIDBreakDownTable(data, mpidFee);
	timeStamp = getTimeStamp();
	model.ExecutionData.push_back(fillExecutionDataTable(data));
	{
		auto breakdowns = fillTradeFeeBreakDownTable(data, feeCommission);
		for (auto breakdown : breakdowns)
		{
			model.TradeFeeBreakDownData.emplace_back(breakdown);
		}
	}
	{
		auto breakdowns = fillMPIDBreakDownTable(data, mpidFee);
		for (auto breakdown : breakdowns)
		{
			model.MPIDFeeBreakDownData.emplace_back(breakdown);
		}
	}

#endif
}

std::string DbDataAdapter::fillTradeFeesAndCommissionsTable(const OrderExecutionData& data, const FeeComissionData& feeCommission)
{
	return"";
}

std::vector<std::string> DbDataAdapter::fillTradeFeeBreakDownTable(const OrderExecutionData& data, const FeeComissionData& feeCommission)
{
	std::vector<std::string> queries;
#ifdef BULK_INSERT


#else
	std::string temp = fmt::format("{},{},{},{},{},{},{},", data.executionDate, data.orderId, data.executionId, data.accountValue, data.date, data.fileName, (int)data.tradeEntryType);

	for (auto itr = feeCommission.tradeFeeBreakDown.begin(); itr != feeCommission.tradeFeeBreakDown.end(); ++itr)
	{
		std::string query = fmt::format("{}{},{},{},{},{},{},1,0,1,System,{}", temp, itr->feeCategoryId, itr->orderFee, itr->executionFee, itr->planId, data.firmId, timeStamp, data.tradeDate);

		queries.push_back(query);
	}
#endif
	return queries;
}

std::string DbDataAdapter::fillMPIDFeesAndCommissionsTable(const OrderExecutionData& data, const FeeComissionData& mpidFee)
{

	//std::string query = fmt::format("{},{},{},{},{},{},{},{},{},{},{},{},1,0,1,System", data.executionDate, data.orderId, data.executionId, data.accountValue, data.date, data.accountId, mpidFee.totalTradeFee.executionFee, mpidFee.totalTradeFee.orderFee,
	//	mpidFee.comission.executionFee, mpidFee.comission.orderFee, data.firmId, timeStamp);
	//return query;
	return "";
}

std::vector<std::string> DbDataAdapter::fillMPIDBreakDownTable(const OrderExecutionData& data, const FeeComissionData& mpidFee)
{

	std::vector<std::string> queries;

#ifdef BULK_INSERT
	std::string temp = fmt::format("{},{},{},{},{},{},{},", data.executionDate, data.orderId, data.executionId, data.accountValue, data.date, data.fileName, (int)data.tradeEntryType);

	for (auto itr = mpidFee.tradeFeeBreakDown.begin(); itr != mpidFee.tradeFeeBreakDown.end(); ++itr)
	{
		std::string query = fmt::format("{},{},{},{},{},{},{},{},1,0,1,System,{}", temp, itr->feeCategoryId, itr->orderFee, itr->executionFee, itr->planId, data.firmId, timeStamp, data.tradeDate);

		queries.push_back(query);
	}

	
#else

	std::string temp = fmt::format("{},{},{},{},{},{},{},", data.executionDate, data.orderId, data.executionId, data.accountValue, data.date, data.fileName, (int)data.tradeEntryType);

	for (auto itr = mpidFee.tradeFeeBreakDown.begin(); itr != mpidFee.tradeFeeBreakDown.end(); ++itr)
	{
		std::string query = fmt::format("{},{},{},{},{},{},{},{},1,0,1,System,{}", temp, itr->feeCategoryId, itr->orderFee, itr->executionFee, itr->planId, data.firmId, timeStamp, data.tradeDate);

		queries.push_back(query);
	}
#endif

	return queries;
}

std::string DbDataAdapter::fillExecutionDataTable(const OrderExecutionData& data)
{

#ifdef BULK_INSERT
	std::string query = fmt::format("{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},1,0,1,System,{}",
		data.executionDate, data.orderId, data.executionId, data.accountId, data.accountValue, data.route, data.penny, data.liq, data.price, data.quantity, data.execBroker,
		data.contra, data.dst, data.tape, data.monthlyVolume, (int)data.type, data.afterHours, data.internalLiq, data.date, (int)data.side, data.symbol, data.execQty,
		data.avgPx, data.lastShares, data.lastPx, (int)data.capacity, data.fillCount, data.currency, data.beforeHours, (int)data.lot, data.time, data.internalRoute,
		data.firmId, timeStamp, data.fileName, data.dfidRecv, data.mpidRecv, (int)data.execTransType, data.mpidMonthlyVolume, data.firmMonthlyVolume, data.lastMarket, (int)data.tradeEntryType, data.tradeDate);

	return query;

#else

	std::string query = fmt::format("{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},1,0,1,System,{}",
		data.executionDate, data.orderId, data.executionId, data.accountId, data.accountValue, data.route, data.penny, data.liq, data.price, data.quantity, data.execBroker,
		data.contra, data.dst, data.tape, data.monthlyVolume, (int)data.type, data.afterHours, data.internalLiq, data.date, (int)data.side, data.symbol, data.execQty,
		data.avgPx, data.lastShares, data.lastPx, (int)data.capacity, data.fillCount, data.currency, data.beforeHours, (int)data.lot, data.time, data.internalRoute,
		data.firmId, timeStamp, data.fileName, data.dfidRecv, data.mpidRecv, (int)data.execTransType, data.mpidMonthlyVolume, data.firmMonthlyVolume, data.lastMarket, (int)data.tradeEntryType, data.tradeDate);

  return query;

#endif
}

int DbDataAdapter::dataRetrievedExecutedOrders(std::vector<std::vector<std::string>>& ret, const std::string& date, const std::string& partial)
{
	std::int64_t offset = 0;
	std::int64_t reserve = 0;
	const int limit = 10000;
	QueryResult res;
	std::string getRows = "SELECT COUNT(*) AS row_count FROM \"ExecutionData\";";
	if (!date.empty()) {
		getRows = "SELECT COUNT(*) AS row_count FROM \"ExecutionData\" WHERE \"ExecutionDate\" = '" + date + "';"; //Arun:Date conditon Added
	}

	//int err = databaseManager_dynamic.fetch_data(getRows, res);
	int err = db_inserter.fetchQuery(getRows, res);
	if (err)
		return 0;
	else
		reserve = std::stoll(res.Data[0][0]);

	ret.reserve(reserve);

	while (true)
	{

		std::string query;
		QueryResult result;
		
		if (date.empty())
		{
			//Incase default parameter value is used read all data available
			query = "SELECT \"OrderId\",\"Quantity\",\"Price\",\"AccountValue\" FROM \"ExecutionData\" ORDER BY \"OrderId\",\"ExecutionId\",\"Date\" LIMIT 10000 OFFSET " + std::to_string(offset) + ";\n\0";
			//auto err = databaseManager_dynamic.fetch_data(query, result);
			auto err = db_inserter.fetchQuery(query, result);

		}
		else
		{
			query += "CREATE TEMP TABLE temp_order_ids (\"OrderId\" text PRIMARY KEY) ON COMMIT DROP;";
			query += "INSERT INTO temp_order_ids(\"OrderId\") VALUES " + partial + "; ";
			query += "SELECT e.\"OrderId\", e.\"Quantity\", e.\"Price\", e.\"AccountValue\" "
				"FROM \"ExecutionData\" e "
				"JOIN temp_order_ids t ON e.\"OrderId\" = t.\"OrderId\" "
				"WHERE e.\"ExecutionDate\" = '" + date + "' "
				"ORDER BY e.\"OrderId\", e.\"ExecutionId\", e.\"Date\" "
				"LIMIT 100000 OFFSET " + std::to_string(offset) + ";";
		
			auto err = db_inserter.fetch_temp_joined_data_pg(partial, date, offset, result);
		}


		offset += 100000;
		
		if (err == 1)
		{
			LogWarning() << __PRETTY_FUNCTION__ << " 'DB Query Failed'";
			//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
			return 1;
		}
		for (const auto& data : result.Data)
		{
			ret.emplace_back(data);
		}

		if (result.Data.size() < limit)
		{
			break;
		}
		LogDebug() << __PRETTY_FUNCTION__ << " 'Executions Data Retrieved From Database'";
	}
	return 0;
}

int DbDataAdapter::checkIfDataExists(const std::string& date, const std::string table, const std::string& accountValues, const std::string& executionId, const std::string& orderId, const std::string& columnName)
{
	// 0 Mean NO DATA or Failure
	// 1 Means Data Exists
	std::string query;
	if(accountValues.empty() && executionId.empty() && orderId.empty() )
	{
		query = fmt::format("SELECT 1 FROM \"{}\" WHERE \"ExecutionDate\" = '{}' LIMIT 1",table, date);
	}
	else if (accountValues.empty() && executionId.empty())
	{
		query = fmt::format("SELECT 1  FROM \"{}\" WHERE \"ExecutionDate\"  = '{}' and \"OrderId\" = '{}' LIMIT 1", table, date, orderId);
	}
	else if (accountValues.empty() && orderId.empty())
	{
		query = fmt::format("SELECT 1 FROM \"{}\" WHERE \"ExecutionDate\"  = '{}' and \"ExecutionId\" = '{}' LIMIT 1", table, date, executionId);
	}
	else if(accountValues.empty())
	{
		query = fmt::format("SELECT 1 FROM \"{}\" WHERE \"ExecutionDate\"  = '{}' and \"OrderId\" = '{}' and \"ExecutionId\" = '{}' LIMIT 1", table, date, orderId,executionId);
	}
	else if (executionId.empty() && orderId.empty())
	{
		query = fmt::format("SELECT 1 FROM \"{}\" WHERE \"ExecutionDate\"  = '{}' and \"{}\" in ({}) LIMIT 1", table, date,columnName, accountValues);
	}
	else
	{
		query = fmt::format("SELECT 1 FROM \"{}\" WHERE \"ExecutionDate\"  = '{}' and \"{}\" in ({}) and \"OrderId\" = '{}' and \"ExecutionId\" = '{}' LIMIT 1", table, date,columnName, accountValues, orderId, executionId);
	}
	QueryResult result;
	//int err = databaseManager_dynamic.fetch_data(query,result);
	int err = db_inserter.fetchQuery(query, result);
	////INFO_LOG("FeeModule", fmt::format("{} - Delete Query: {} - Err: {}", __PRETTY_FUNCTION__,query,err));
	if(err == 1)
	{
		////INFO_LOG("FeeModule", fmt::format("{} - Data Check Query Failed", __PRETTY_FUNCTION__));
	}
	else
	{
		if (result.Data.size() > 0)
		{
			////INFO_LOG("FeeModule", fmt::format("{} - {}:{} Rows Exists", __PRETTY_FUNCTION__, table, result.Data.at(0).at(0)));
			return result.Data.at(0).at(0) > "0" ? 1 : 0;
		}
	}
	return err;
}
int DbDataAdapter::checkIfDataExistsFileName(const std::string& date, const std::string table, std::string& fileNames)
{
	// 0 Mean NO DATA or Failure
// 1 Means Data Exists
	if (fileNames.empty())
		return 0;
	std::string query =
		"SELECT 1 "
		"FROM \"" + table + "\" "
		"WHERE \"ExecutionDate\" = '" + date + "' "
		"AND \"FileName\" = ANY(ARRAY[" + fileNames + "]) LIMIT 1";

	QueryResult result;
	//int err = databaseManager_dynamic.fetch_data(query, result);
	int err = db_inserter.fetchQuery(query, result);
	////INFO_LOG("FeeModule", fmt::format("{} - Delete Query: {} - Err: {}", __PRETTY_FUNCTION__, query, err));
	if (err == 1)
	{
		////INFO_LOG("FeeModule", fmt::format("{} - Data Check Query Failed", __PRETTY_FUNCTION__));
	}
	else
	{
		if (result.Data.size() > 0)
		{
			////INFO_LOG("FeeModule", fmt::format("{} - {}:{} Rows Exists", __PRETTY_FUNCTION__, table, result.Data.at(0).at(0)));
			return result.Data.at(0).at(0) > "0" ? 1 : 0;
		}
	}
	return err;
}
int DbDataAdapter::cleanAccountTransactData(const std::string& date, const std::string& accountValues, const std::string& executionId, const std::string& orderId)
{
	//INFO_LOG("FeeModule", fmt::format("{} - DELETING DATA", __PRETTY_FUNCTION__));
	std::string query;
	int err = 1;
	try {
		err = checkIfDataExists(date, "TradeFeeBreakDown", accountValues, executionId, orderId);
		if (err > 0)
		{
			query = "DELETE FROM \"TradeFeeBreakDown\" \
			USING \"ExecutionData\" \
			WHERE \"TradeFeeBreakDown\".\"OrderId\" = \"ExecutionData\".\"OrderId\" \
			AND \"TradeFeeBreakDown\".\"AccountValue\" = \"ExecutionData\".\"AccountValue\" \
			AND \"ExecutionData\".\"ExecutionDate\" >= '" +
					date + "'::date\
            AND \"ExecutionData\".\"ExecutionDate\" < ('" + date + "'::date + INTERVAL '1 day') \
			AND \"ExecutionData\".\"AccountValue\" in (" +
					accountValues + ") \
			AND \"ExecutionData\".\"OrderId\" = '" +
					orderId + "' \
			AND \"ExecutionData\".\"ExecutionId\" = '" +
					executionId + "';\n";
			err = db_inserter.executeQuery(query);
			////INFO_LOG("FeeModule", fmt::format("{} - DELETED TradeFeeBreakDown", __PRETTY_FUNCTION__));
			if (err)
			{
				throw std::string("TradeFeeBreakDown Deletion Failed");
			}
		}


			err = checkIfDataExists(date, "MPIDFeeBreakDown", accountValues, executionId, orderId);
			if (err > 0)
			{
				query = "DELETE FROM \"MPIDFeeBreakDown\" \
			USING \"ExecutionData\" \
			WHERE \"MPIDFeeBreakDown\".\"OrderId\" = \"ExecutionData\".\"OrderId\" \
			AND \"MPIDFeeBreakDown\".\"AccountValue\" = \"ExecutionData\".\"AccountValue\" \
			AND \"ExecutionData\".\"ExecutionDate\" >= '" +
					date + "'::date\
            AND \"ExecutionData\".\"ExecutionDate\" < ('" + date + "'::date + INTERVAL '1 day') \
			AND \"ExecutionData\".\"AccountValue\" in (" +
						accountValues + ") \
			AND \"ExecutionData\".\"OrderId\" = '" +
						orderId + "' \
			AND \"ExecutionData\".\"ExecutionId\" = '" +
						executionId + "';\n";
				err = db_inserter.executeQuery(query);
				////INFO_LOG("FeeModule", fmt::format("{} - DELETED MPIDFeeBreakDown", __PRETTY_FUNCTION__));
				if (err)
				{
					throw std::string("MPIDFeeBreakDown Deletion Failed");
				}
			}
			else
			{
				////INFO_LOG("FeeModule", fmt::format("{} - No data with specified parameters in MPIDFeeBreakDown", __PRETTY_FUNCTION__));
			}
			err = checkIfDataExists(date, "ExecutionData", accountValues, executionId, orderId);
			if (err > 0)
			{
			 query =
					"DELETE FROM \"ExecutionData\" "
					"WHERE \"ExecutionDate\" >= '" + date + "'::date "
					        "AND \"ExecutionDate\" < ('" + date + "'::date + INTERVAL '1 day') " 
					"AND \"AccountValue\" IN (" + accountValues + ") "
					"AND \"OrderId\" = '" + orderId + "' "
					"AND \"ExecutionId\" = '" + executionId + "';";

			 err = db_inserter.executeQuery(query);
				//INFO_LOG("FeeModule", fmt::format("{} - DELETED ExecutionData", __PRETTY_FUNCTION__));
				if (err)
				{
					throw std::string("ExecutionData Deletion Failed");
				}
			}
			else
			{
				////INFO_LOG("FeeModule", fmt::format("{} - No data with specified parameters in ExecutionData", __PRETTY_FUNCTION__));
			}
		}
	catch (const std::string& message)
	{
	
		//WARNING_LOG("FCM-Engine", fmt::format("{} - Unable To Delete Data For: {}", __PRETTY_FUNCTION__, message));
		////throw std::runtime_error(fmt::format("cleanAccountTransactData: {}", __PRETTY_FUNCTION__, message));
	}
	
	return 0;
}
int DbDataAdapter::deleteExecutionDataOptimized(const std::string& tableName,const std::string& execDate,const std::string& columnName,const std::string& fileNames) // This function is used to delete in EOD and dropCopy flows, earlier simple without CTE based implementation used to stuck while deleting data
{
	std::string query;

	query =
		"WITH params AS ( "
		"  SELECT "
		"    DATE '" + execDate + "' AS exec_date, "
		"    ARRAY[" + fileNames + "]::text[] AS file_names "
		"), "
		"bounds AS ( "
		"  SELECT "
		"    exec_date, "
		"    file_names, "
		"    (exec_date - 1) AS date_from, "
		"    (exec_date + 1) AS date_to, "
		"    exec_date       AS exec_from, "
		"    (exec_date + 1) AS exec_to "
		"  FROM params "
		"), "
		"todel AS ( "
		"  SELECT ed.tableoid AS part_oid, ed.ctid AS part_ctid "
		"  FROM public.\"" + tableName + "\" ed "
		"  CROSS JOIN bounds b "
		"  WHERE ed.\"ExecutionDate\" >= b.exec_from "
		"    AND ed.\"ExecutionDate\" <  b.exec_to "
		"    AND ed.\"" + columnName + "\" = ANY(b.file_names) "
		"    AND ed.\"Date\" >= b.date_from "
		"    AND ed.\"Date\" <=  b.date_to "
		") "
		"DELETE FROM public.\"" + tableName + "\" ed "
		"USING todel, bounds b "
		"WHERE ed.tableoid = todel.part_oid "
		"  AND ed.ctid    = todel.part_ctid "
		"  AND ed.\"ExecutionDate\" >= b.exec_from "
		"  AND ed.\"ExecutionDate\" <  b.exec_to "
		"  AND ed.\"Date\" >= b.date_from "
		"  AND ed.\"Date\" <=  b.date_to "
		"  AND ed.\"" + columnName + "\" = ANY(b.file_names);";

	////INFO_LOG("FeeModule",
	//	fmt::format("{} - Executing optimized delete on {}", __PRETTY_FUNCTION__, tableName));

	return db_inserter.executeQuery(query);
}

int DbDataAdapter::cleanAccountTransactDataFileName(const std::string& date, std::string& fileNames, const std::string& columnName)
{
	// LogInfo() << __PRETTY_FUNCTION__ << " 'DELETING DATA'";
	//INFO_LOG("FeeModule", fmt::format("{} - DELETING DATA", __PRETTY_FUNCTION__));
	std::string query;
	int err = 1;
	try
	{
			err = checkIfDataExistsFileName(date, "TradeFeeBreakDown", fileNames);
			if (err > 0)
			{
				err = deleteExecutionDataOptimized("TradeFeeBreakDown",date,columnName,fileNames);
				//INFO_LOG("FeeModule", fmt::format("{} - DELETED TradeFeeBreakDown", __PRETTY_FUNCTION__));
				if (err)
				{
					throw std::string("TradeFeeBreakDown Deletion Failed");
				}
			}
			err = checkIfDataExistsFileName(date, "MPIDFeeBreakDown", fileNames);
			if (err > 0)
			{
				err = deleteExecutionDataOptimized("MPIDFeeBreakDown", date, columnName, fileNames);
				//INFO_LOG("FeeModule", fmt::format("{} - DELETED MPIDFeeBreakDown", __PRETTY_FUNCTION__));
				if (err)
				{
					throw std::string("MPIDFeeBreakDown Deletion Failed");
				}
			}
			else
			{
				//INFO_LOG("FeeModule", fmt::format("{} - No data with specified parameters in MPIDFeeBreakDown", __PRETTY_FUNCTION__));
			}
			err = checkIfDataExistsFileName(date, "ExecutionData", fileNames);
			if (err > 0)
			{

				err = deleteExecutionDataOptimized("ExecutionData", date, columnName, fileNames);
				//INFO_LOG("FeeModule", fmt::format("{} - DELETED ExecutionData", __PRETTY_FUNCTION__));
				if (err)
				{
					throw std::string("ExecutionData Deletion Failed");
				}
			}
			else
			{
				//INFO_LOG("FeeModule", fmt::format("{} - No data with specified parameters in ExecutionData", __PRETTY_FUNCTION__));
			}
		

	}
	catch (const std::string& message)
	{
		//WARNING_LOG("FCM-Engine", fmt::format("{} - Unable To Delete Data For: {}", __PRETTY_FUNCTION__, message));
		//throw std::runtime_error(fmt::format("cleanAccountTransactData: {}", __PRETTY_FUNCTION__, message));
	}
	return 0;
}
int DbDataAdapter::cleanAccountTransactData(const std::string& date, const std::string& accountValues, const std::string& columnName)
{
	// LogInfo() << __PRETTY_FUNCTION__ << " 'DELETING DATA'";
	//INFO_LOG("FeeModule", fmt::format("{} - DELETING DATA", __PRETTY_FUNCTION__));
	std::string query;
	int err = 1;
	try
	{
		err = checkIfDataExists(date, "TradeFeeBreakDown", accountValues,"","",columnName);
		if (err > 0)
		{
			err = deleteExecutionDataOptimized("TradeFeeBreakDown", date, columnName, accountValues);
			//INFO_LOG("FeeModule", fmt::format("{} - DELETED TradeFeeBreakDown", __PRETTY_FUNCTION__));
			if (err)
			{
				throw std::string("TradeFeeBreakDown Deletion Failed");
			}
		}
			err = checkIfDataExists(date, "MPIDFeeBreakDown", accountValues, "", "", columnName);
			if (err > 0)
			{
				err = deleteExecutionDataOptimized("MPIDFeeBreakDown", date, columnName, accountValues);
				//INFO_LOG("FeeModule", fmt::format("{} - DELETED MPIDFeeBreakDown", __PRETTY_FUNCTION__));
				if (err)
				{
					throw std::string("MPIDFeeBreakDown Deletion Failed");
				}
			}
			else
			{
				//INFO_LOG("FeeModule", fmt::format("{} - No data with specified parameters in MPIDFeeBreakDown", __PRETTY_FUNCTION__));
			}
		}
	catch (const std::string& message)
	{
		//WARNING_LOG("FCM-Engine", fmt::format("{} - Unable To Delete Data For: {}", __PRETTY_FUNCTION__, message));
		//throw std::runtime_error(fmt::format("cleanAccountTransactData: {}", __PRETTY_FUNCTION__, message));
	}
	return 0;
}

int DbDataAdapter::cleanMpidTransactData(const std::string& date, const std::string& mpidIds, bool deleteAll)
{
	//INFO_LOG("FeeModule", fmt::format("{} - DELETING DATA", __PRETTY_FUNCTION__));
	int err = 1;
	std::string query;
	std::string accountValues = "(SELECT UPPER(\"Value\") FROM \"Accounts\" WHERE \"MPIDId\" in (" + mpidIds + "))";


	query = "DELETE FROM \"MPIDFeeBreakDown\" "
		"WHERE \"ExecutionDate\" >= '" + date + "'::date "
		"AND \"ExecutionDate\" < ('" + date + "'::date + INTERVAL '1 day') "
		"AND \"AccountValue\" in " + accountValues + ";\n";

	//err = databaseManager_dynamic.execute_query(query);
	 err = db_inserter.executeQuery(query);

	query = "DELETE FROM \"MPIDFeesAndCommissions\" "
		"WHERE \"ExecutionDate\" >= '" + date + "'::date "
		"AND \"ExecutionDate\" < ('" + date + "'::date + INTERVAL '1 day') "
		"AND \"AccountValue\" in " + accountValues + ";\n";
	//err = databaseManager_dynamic.execute_query(query);
	 err = db_inserter.executeQuery(query);

	if (deleteAll)
	{
		query = "DELETE FROM \"TradeFeeBreakDown\" "
			"WHERE \"ExecutionDate\" >= '" + date + "'::date "
			"AND \"ExecutionDate\" < ('" + date + "'::date + INTERVAL '1 day') "
			"AND \"AccountValue\" in " + accountValues + ";\n";

		//err = databaseManager_dynamic.execute_query(query);
		 err = db_inserter.executeQuery(query);
		query = "DELETE FROM \"TradeFeeBreakDown\" "
			"WHERE \"ExecutionDate\" >= '" + date + "'::date "
			"AND \"ExecutionDate\" < ('" + date + "'::date + INTERVAL '1 day') "
			"AND \"AccountValue\" in " + accountValues + ";\n";

		//err = databaseManager_dynamic.execute_query(query);
		 err = db_inserter.executeQuery(query);
		query = "DELETE FROM \"ExecutionData\" "
			"WHERE \"ExecutionDate\" >= '" + date + "'::date "
			"AND \"ExecutionDate\" < ('" + date + "'::date + INTERVAL '1 day') "
			"AND \"AccountValue\" in " + accountValues + ";\n";

		/*err = databaseManager_dynamic.execute_query(query);*/
		 err = db_inserter.executeQuery(query);
	}

	return 0;
}

void DbDataAdapter::BulkExecuteWorkFlow()
{

	//INFO_LOG("FeeModule", fmt::format("{} - BULK EXECUTION STARTED", __PRETTY_FUNCTION__));
	int err = 1;
	std::string thread1Exception = "";
	std::string thread2Exception = "";
	try {
		err = db_inserter.upsertFromCSV(model.ExecutionData, "ExecutionData", model.getColumns("ExecutionData"), "\"ExecutionId\",\"OrderId\",\"Date\",\"AccountValue\"");
	
		if (err)
		{
			std::cerr << "ExecutionData: Bulk Insertion Failed\n";

			//WARNING_LOG("FeeModule", fmt::format("{} - ExecutionData: Bulk Insertion Failed", __PRETTY_FUNCTION__));
			throw std::string("ExecutionData: Bulk Insertion Failed");
		}
		else {

			//INFO_LOG("FeeModule", fmt::format("{} - DELETING DATA", __PRETTY_FUNCTION__));
		}

		// Process first chunk
		std::vector<std::string>& fullData_Mpid = model.MPIDFeeBreakDownData;

		// Split point 
		size_t mid_mpid = fullData_Mpid.size() / 2;

		// Create sub-vectors
		std::vector<std::string> MpidFeeBreakDownData_1(fullData_Mpid.begin(), fullData_Mpid.begin() + mid_mpid);
		std::vector<std::string> MpidFeeBreakDownData_2(fullData_Mpid.begin() + mid_mpid, fullData_Mpid.end());

		model.MPIDFeeBreakDownData.clear();
		//INFO_LOG("FeeModule", fmt::format("{} - SUB-VECTORS CREATED FOR MPID FEE BREAKDOWN", __PRETTY_FUNCTION__));
		//INFO_LOG("FeeModule", fmt::format("{} - CALLING Mpid Fee Vector 1 with size : {}", __PRETTY_FUNCTION__, MpidFeeBreakDownData_1.size()));
		err = db_inserter.insertFromCSV(MpidFeeBreakDownData_1, "MPIDFeeBreakDown", model.getColumns("MPIDFeeBreakDown"));
		if (err)
		{
			std::cerr << "MPIDFeeBreakDown (Chunk 1): Bulk Insertion Failed\n";
			//WARNING_LOG("FeeModule", fmt::format("{} - MPIDFeeBreakDown (Chunk 1): Bulk Insertion Failed", __PRETTY_FUNCTION__));
			thread2Exception = std::string("MPIDFeeBreakDown (Chunk 1): Bulk Insertion Failed");
		}
		else
		{

			//INFO_LOG("FeeModule", fmt::format("{} - MPIDFeeBreakDown Added To DB Successfully", __PRETTY_FUNCTION__));
		}
		//INFO_LOG("FeeModule", fmt::format("{} - CALLING Mpid Fee Vector 2 with size : {}", __PRETTY_FUNCTION__, MpidFeeBreakDownData_2.size()));
		err = db_inserter.insertFromCSV(MpidFeeBreakDownData_2, "MPIDFeeBreakDown", model.getColumns("MPIDFeeBreakDown"));

		if (err)
		{
			std::cerr << "MPIDFeeBreakDown (Chunk 2): Bulk Insertion Failed\n";
			//WARNING_LOG("FeeModule", fmt::format("{} - MPIDFeeBreakDown (Chunk 2): Bulk Insertion Failed", __PRETTY_FUNCTION__));
			thread2Exception = std::string("MPIDFeeBreakDown (Chunk 1): Bulk Insertion Failed");
		}
		else
		{

			//INFO_LOG("FeeModule", fmt::format("{} - MPIDFeeBreakDown Added To DB Successfully", __PRETTY_FUNCTION__));
		}
		std::vector<std::string>& fullData = model.TradeFeeBreakDownData;

		// Split point (halfway, or adjust as needed)
		size_t mid = fullData.size() / 2;

		// Create two sub-vectors
		std::vector<std::string> TradeFeeBreakDownData_1(fullData.begin(), fullData.begin() + mid);
		std::vector<std::string> TradeFeeBreakDownData_2(fullData.begin() + mid, fullData.end());
		model.TradeFeeBreakDownData.clear();
		//INFO_LOG("FeeModule", fmt::format("{} - CALLING Trade Fee Vector 1 with size : {}", __PRETTY_FUNCTION__, TradeFeeBreakDownData_1.size()));
		err = db_inserter.insertFromCSV(TradeFeeBreakDownData_1, "TradeFeeBreakDown", model.getColumns("TradeFeeBreakDown"));
		if (err)
		{
			std::cerr << "TradeFeeBreakDown (Chunk 1): Bulk Insertion Failed\n";
			//WARNING_LOG("FeeModule", fmt::format("{} - TradeFeeBreakDown (Chunk 1): Bulk Insertion Failed", __PRETTY_FUNCTION__));
			thread1Exception = std::string("TradeFeeBreakDown (Chunk 1): Bulk Insertion Failed");
		}
		else
		{
			//INFO_LOG("FeeModule", fmt::format("{} - TradeFeeBreakDown (Chunk 1) Added To DB Successfully", __PRETTY_FUNCTION__));
		}

		// Process second chunk
		//INFO_LOG("FeeModule", fmt::format("{} - CALLING Trade Fee Vector 2 with size : {}", __PRETTY_FUNCTION__, TradeFeeBreakDownData_2.size()));
		err = db_inserter.insertFromCSV(TradeFeeBreakDownData_2, "TradeFeeBreakDown", model.getColumns("TradeFeeBreakDown"));
		if (err)
		{
			std::cerr << "TradeFeeBreakDown (Chunk 2): Bulk Insertion Failed\n";
			//WARNING_LOG("FeeModule", fmt::format("{} - TradeFeeBreakDown (Chunk 2): Bulk Insertion Failed", __PRETTY_FUNCTION__));
			thread1Exception = std::string("TradeFeeBreakDown (Chunk 2): Bulk Insertion Failed");
		}
		else
		{
			//INFO_LOG("FeeModule", fmt::format("{} - TradeFeeBreakDown (Chunk 2) Added To DB Successfully", __PRETTY_FUNCTION__));
		}

		//INFO_LOG("FeeModule", fmt::format("{} - TDone Adding To Database", __PRETTY_FUNCTION__));
		if (!thread1Exception.empty())
		{
			throw thread1Exception;
		}
		else if (!thread2Exception.empty())
		{
			throw thread2Exception;
		}
	}
	catch (const std::string& message)
	{

		//WARNING_LOG("FCM-Engine", fmt::format("{} - Unable To Delete Data For: {}", __PRETTY_FUNCTION__, message));
		//throw std::runtime_error(fmt::format("BulkExecute: {}", __PRETTY_FUNCTION__, message));
	}
}

void DbDataAdapter::BulkExecute()
{
	//INFO_LOG("FeeModule", fmt::format("{} - BULK EXECUTION STARTED", __PRETTY_FUNCTION__));
	int err = 1;
	try {
		// Resolve partition names from the execution date in the first row
		std::string execDate = todayDate; // YYYY-MM-DD format
		std::string execPartition = BulkInserter::resolvePartitionName("ExecutionData", execDate);
		std::string tradePartition = BulkInserter::resolvePartitionName("TradeFeeBreakDown", execDate);
		std::string mpidPartition = BulkInserter::resolvePartitionName("MPIDFeeBreakDown", execDate);

		//INFO_LOG("FeeModule", fmt::format("{} - Targeting partitions: {}, {}, {}", __PRETTY_FUNCTION__, execPartition, tradePartition, mpidPartition));

		err = db_inserter.insertFromCSV(model.ExecutionData, "ExecutionData", model.getColumns("ExecutionData"));
	if (err)
	{
		std::cerr << "ExecutionData: Bulk Insertion Failed\n";
		
		//WARNING_LOG("FeeModule", fmt::format("{} - ExecutionData: Bulk Insertion Failed", __PRETTY_FUNCTION__));
			throw std::string("ExecutionData: Bulk Insertion Failed");
	}
	else {
		
		//INFO_LOG("FeeModule", fmt::format("{} - DELETING DATA", __PRETTY_FUNCTION__));
	}
	// Process first chunk
	std::vector<std::string>& fullData_Mpid = model.MPIDFeeBreakDownData;

	// Split point 
	size_t mid_mpid = fullData_Mpid.size() / 2;

	// Create sub-vectors
	std::vector<std::string> MpidFeeBreakDownData_1(fullData_Mpid.begin(), fullData_Mpid.begin() + mid_mpid);
	std::vector<std::string> MpidFeeBreakDownData_2(fullData_Mpid.begin() + mid_mpid, fullData_Mpid.end());

	model.MPIDFeeBreakDownData.clear();
	INFO_LOG("FeeModule", fmt::format("{} - CALLING Mpid Fee Vector 1 with size : {}", __PRETTY_FUNCTION__, MpidFeeBreakDownData_1.size()));
	err = db_inserter.insertFromCSV(MpidFeeBreakDownData_1, "MPIDFeeBreakDown", model.getColumns("MPIDFeeBreakDown"));
		if (err)
		{
			std::cerr << "MPIDFeeBreakDownData: Bulk Insertion Failed\n";
			//WARNING_LOG("FeeModule", fmt::format("{} - MPIDFeeBreakDownData: Bulk Insertion Failed", __PRETTY_FUNCTION__));
			throw std::string("MPIDFeeBreakDownData: Bulk Insertion Failed");
		}
		else
		{
		
		//INFO_LOG("FeeModule", fmt::format("{} - MPIDFeeBreakDown Added To DB Successfully", __PRETTY_FUNCTION__));
		}
		err = db_inserter.insertFromCSV(MpidFeeBreakDownData_2, "MPIDFeeBreakDown", model.getColumns("MPIDFeeBreakDown"));
		if (err)
		{
			std::cerr << "MPIDFeeBreakDownData: Bulk Insertion Failed\n";
			//WARNING_LOG("FeeModule", fmt::format("{} - MPIDFeeBreakDownData: Bulk Insertion Failed", __PRETTY_FUNCTION__));
			throw std::string("MPIDFeeBreakDownData: Bulk Insertion Failed");
		}
		else
		{

			//INFO_LOG("FeeModule", fmt::format("{} - MPIDFeeBreakDown Added To DB Successfully", __PRETTY_FUNCTION__));
		}
		std::vector<std::string>& fullData = model.TradeFeeBreakDownData;

		// Split point (halfway, or adjust as needed)
		size_t mid = fullData.size() / 2;

		// Create two sub-vectors
		std::vector<std::string> TradeFeeBreakDownData_1(fullData.begin(), fullData.begin() + mid);
		std::vector<std::string> TradeFeeBreakDownData_2(fullData.begin() + mid, fullData.end());
		model.TradeFeeBreakDownData.clear();
	err = db_inserter.insertFromCSV(TradeFeeBreakDownData_1, "TradeFeeBreakDown", model.getColumns("TradeFeeBreakDown"));
	if (err)
	{
		std::cerr << "TradeFeeBreakDownData: Bulk Insertion Failed\n";
		//WARNING_LOG("FeeModule", fmt::format("{} - TradeFeeBreakDownData: Bulk Insertion Failed", __PRETTY_FUNCTION__));
		throw std::string("TradeFeeBreakDownData: Bulk Insertion Failed");
	}
	else
	{
		//INFO_LOG("FeeModule", fmt::format("{} - TradeFeeBreakDown (Chunk 1) Added To DB Successfully", __PRETTY_FUNCTION__));
	}
	err = db_inserter.insertFromCSV(TradeFeeBreakDownData_2, "TradeFeeBreakDown", model.getColumns("TradeFeeBreakDown"));
	if (err)
	{
		std::cerr << "TradeFeeBreakDownData: Bulk Insertion Failed\n";
		//WARNING_LOG("FeeModule", fmt::format("{} - TradeFeeBreakDownData: Bulk Insertion Failed", __PRETTY_FUNCTION__));
		throw std::string("TradeFeeBreakDownData: Bulk Insertion Failed");
	}
	else
	{
		//INFO_LOG("FeeModule", fmt::format("{} - TradeFeeBreakDown (Chunk 1) Added To DB Successfully", __PRETTY_FUNCTION__));
		}	
	}
	catch (const std::string& message)
	{
	
		//WARNING_LOG("FCM-Engine", fmt::format("{} - Unable To Delete Data For: {}", __PRETTY_FUNCTION__, message));
		//throw std::runtime_error(fmt::format("BulkExecute: {}", __PRETTY_FUNCTION__, message));
	}
}

std::string DbDataAdapter::getTimeStamp()
{
	std::string timestampString = "";
	boost::posix_time::ptime currentTime = boost::posix_time::microsec_clock::local_time();
	timestampString = boost::posix_time::to_iso_extended_string(currentTime);

	timestampString = timestampString.substr(0, timestampString.find('T')) + ' ' +
		timestampString.substr(timestampString.find('T') + 1);
	return timestampString;
}


void DbDataAdapter::finish()
{
	if (instance)
	{
		delete instance;
		instance = nullptr;
	}
}

//==================================== BACK DATED ========================================

int DbDataAdapter::RetrieveMpidAccounts(std::vector<Account>& accounts, const std::string& mpidIds, const AdjustmentType type)
{
	std::string query = "";

	if (type == EN_Adjustment_MPIDFee_Mpid || type == EN_Adjustment_TradeFee_Account || type == EN_Adjustment_TradeFee_Mpid || type == EN_Adjustment_Add_Execution)
	{
		std::string mpidPart = "'" + mpidIds + "'";

		query = "SELECT \"Id\", \"Value\", \"FirmId\", \"MPIDId\" FROM \"Accounts\" WHERE \"MPIDId\" = " +
			mpidPart + " AND \"IsActive\" = true AND \"IsDeleted\" = false;\n\0";
	}

	else if (type == EN_Adjustment_MPIDFee_MultipleMpids || type == EN_Adjustment_TradeFee_MultipleMpids)
	{
		query = "SELECT \"Id\", \"Value\", \"FirmId\", \"MPIDId\" FROM \"Accounts\" WHERE \"MPIDId\" in (" +
			mpidIds + ") AND \"IsActive\" = true AND \"IsDeleted\" = false;\n\0";
	}

	else
	{
		query = "INVALID QUERY COZ OF INVALID ADJUSTMENT TYPE";
	}


	QueryResult result;
	int err = fetchDataFromStaticDb(query, result);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		return 1;
	}
	else
	{
		for (int i = 0; i < result.Data.size(); i++)
		{
			Account a1;
			a1.id = result.Data[i][0];
			boost::algorithm::to_upper(result.Data[i][1]); 
			a1.name = result.Data[i][1];
			a1.firmId = result.Data[i][2];
			a1.mpidId = result.Data[i][3];
			accounts.emplace_back(a1);
		}
	}
	DEBUG_LOG("FeeModule", fmt::format("{} - Accounts & their Groups Retrieved From Database", __PRETTY_FUNCTION__));

	return 0;
}

int DbDataAdapter::RetrievePlanAccounts(std::vector<Account>& accounts, const std::string& planIds, const AdjustmentType type)
{
	std::unordered_set<std::string> temp_accounts;

	std::string query = "SELECT DISTINCT \"A\".\"Id\", \"A\".\"Value\", \"A\".\"FirmId\", \"A\".\"MPIDId\" \
	FROM \"Accounts\" AS \"A\" \
	JOIN \"TradeFees\" AS \"TF\" ON \"A\".\"Id\" = \"TF\".\"AccountId\" \
	JOIN \"Plans\" AS \"P\" ON \"TF\".\"PlanId\" = \"P\".\"Id\" \
	WHERE \"P\".\"Id\" IN (" + planIds + ");";

	QueryResult result;
	//int err = databaseManager_static.fetch_data(query, result);
	int err = db_inserter.fetchQuery(query, result);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		return 1;
	}
	else
	{
		for (int i = 0; i < result.Data.size(); i++)
		{
			Account a1;
			a1.id = result.Data[i][0];
			boost::algorithm::to_upper(result.Data[i][1]); //Normalizing Account Value
			a1.name = result.Data[i][1];
			a1.firmId = result.Data[i][2];
			a1.mpidId = result.Data[i][3];
			if (temp_accounts.count(a1.name) == 0) //to prevent duplicates
			{
				accounts.emplace_back(a1);
				temp_accounts.insert(a1.name);
			}
		}

		query = "SELECT DISTINCT  a.\"Id\", a.\"Value\", a.\"FirmId\", a.\"MPIDId\"  \
		FROM \"TradeFees\" tf \
		JOIN \"Firms\" f ON tf.\"FirmId\" = f.\"Id\" \
		JOIN \"Accounts\" a ON a.\"FirmId\" = f.\"Id\" \
		WHERE tf.\"PlanId\" IN (" + planIds + ");";

		//err = databaseManager_static.fetch_data(query, result);
		int err = db_inserter.fetchQuery(query, result);
		if (err == 1)
		{
			//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
			//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
			return 1;
		}
		else
		{
			for (int i = 0; i < result.Data.size(); i++)
			{
				Account a1;
				a1.id = result.Data[i][0];
				boost::algorithm::to_upper(result.Data[i][1]); //Normalizing Account Value
				a1.name = result.Data[i][1];
				a1.firmId = result.Data[i][2];
				a1.mpidId = result.Data[i][3];
				if (temp_accounts.count(a1.name) == 0)
				{
					accounts.emplace_back(a1);
					temp_accounts.insert(a1.name);
				}
			}

			query = "SELECT DISTINCT  a.\"Id\", a.\"Value\", a.\"FirmId\", a.\"MPIDId\" \
			FROM \"TradeFees\" tf \
			JOIN \"MPID\" m on tf.\"MPIDId\" = m.\"Id\" \
			JOIN \"Accounts\" a on a.\"MPIDId\" = m.\"Id\" \
			WHERE tf.\"PlanId\" in (" + planIds + ");";

			//err = databaseManager_static.fetch_data(query, result);
			int err = db_inserter.fetchQuery(query, result);
			if (err == 1)
			{
				//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
				//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
				return 1;
			}
			else
			{
				for (int i = 0; i < result.Data.size(); i++)
				{
					Account a1;
					a1.id = result.Data[i][0];
					boost::algorithm::to_upper(result.Data[i][1]); //Normalizing Account Value
					a1.name = result.Data[i][1];
					a1.firmId = result.Data[i][2];
					a1.mpidId = result.Data[i][3];
					if (temp_accounts.count(a1.name) == 0)
					{
						accounts.emplace_back(a1);
						temp_accounts.insert(a1.name);
					}
				}
			}
		}
		return 0;
	}
}

int DbDataAdapter::RetrieveFirmAccounts(std::vector<Account>& accounts, const std::string& firmIds, const AdjustmentType type)
{
	std::string query = "";

	if (type == EN_Adjustment_TradeFee_Firm || type == EN_Adjustment_TradeFee_MultpleFirms)
	{
		query = "SELECT \"Id\", \"Value\", \"FirmId\", \"MPIDId\" FROM \"Accounts\" WHERE \"FirmId\" in (" +
			firmIds + ") AND \"IsActive\" = true AND \"IsDeleted\" = false;\n\0";
	}

	else
	{
		query = "INVALID QUERY COZ OF INVALID ADJUSTMENT TYPE";
	}


	QueryResult result;
	int err = fetchDataFromStaticDb(query, result);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		return 1;
	}
	else
	{
		for (int i = 0; i < result.Data.size(); i++)
		{
			Account a1;
			a1.id = result.Data[i][0];
			boost::algorithm::to_upper(result.Data[i][1]); //Normalizing Account Value
			a1.name = result.Data[i][1];
			a1.firmId = result.Data[i][2];
			a1.mpidId = result.Data[i][3];
			accounts.emplace_back(a1);
		}
	}
	DEBUG_LOG("FeeModule", fmt::format("{} - Accounts & their Groups Retrieved From Database", __PRETTY_FUNCTION__));
	return 0;
}

int DbDataAdapter::RetrieveSpecificMonthlyDetails(std::vector<MonthlyDetails>& monthlyDetailsData, const std::string& mpidId, const std::string& date)
{
	std::string query{ "" };
	if (date.empty())
	{
		query = "SELECT \"AccountValue\", \"MonthlyVolume\", \"TradeType\" FROM \"MonthlyDetails\" Where CAST(\"Date\" AS Date) = (SELECT CAST(MAX(\"Date\") AS Date) From \"MonthlyDetails\") AND \"AccountValue\" in (Select \"Value\" From \"Accounts\" Where \"MPIDId\" = '" + mpidId + "');";
	}
	else
	{
		std::string wherePart = "(SELECT CAST(MAX(\"Date\") AS Date) From \"MonthlyDetails\" WHERE \"Date\" < '" + date + "') ";
		query = "SELECT \"AccountValue\", \"MonthlyVolume\", \"TradeType\" FROM \"MonthlyDetails\" Where CAST(\"Date\" AS Date) = " + wherePart +
			"AND \"AccountValue\" in (Select \"Value\" From \"Accounts\" Where \"MPIDId\" = '" + mpidId + "');";
	}
	QueryResult result;
	//int err = databaseManager_dynamic.fetch_data(query, result);
	int err = db_inserter.fetchQuery(query, result);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		return 1;
	}
	else
	{
		for (const auto& data : result.Data)
		{
			MonthlyDetails details;
			details.accountValue = data[0];
				details.monthlyVolume = std::stod(data[1]);
			details.tradeType = std::stoi(data[2]);
			monthlyDetailsData.emplace_back(details);
		}
	}
	DEBUG_LOG("FeeModule", fmt::format("{} - Monthly Details Retrieved From Database", __PRETTY_FUNCTION__));
	return 0;
}

int DbDataAdapter::RetrieveRelevantPlans(std::vector <Plan>& plans, const Adjustment& adjustment)
{
	std::string Ids = "'" + adjustment.accountId + "','" + adjustment.mpidId + "'";
	std::string query = "SELECT \"Id\", \"Name\", \"FeeCategoryId\", \"RoundingPoints\", \"PlanType\", \"Rounding\", \"FirmId\", \"FeeCategoryType\", \"ParentPlanId\" FROM \"Plans\" WHERE \"Id\" in (Select \"PlanId\" from \"TradeFees\" Where \"AccessId\" in (" + Ids + ")) AND \"IsActive\" = true;\n\0";
	QueryResult result;
	int err = fetchDataFromStaticDb(query, result);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		return 1;
	}
	else
	{
		for (int i = 0; i < result.Data.size(); i++)
		{
			Plan p1;
			p1.id = result.Data[i][0];
			p1.name = result.Data[i][1];
			p1.feeCategoryId = result.Data[i][2];
			p1.roundingPoints = std::stoi(result.Data[i][3]);
			switch (std::stoi(result.Data[i][4]))
			{
			case EN_PlanType_None:
				p1.planType = EN_PlanType_None;
				break;
			case EN_PlanType_PerOrder:
				p1.planType = EN_PlanType_PerOrder;
				break;
			case EN_PlanType_PerExecution:
				p1.planType = EN_PlanType_PerExecution;
				break;
			}

			switch (std::stoi(result.Data[i][5]))
			{
			case EN_RoundingScheme_HalfUp:
				p1.rounding = EN_RoundingScheme_HalfUp;
				break;
			case EN_RoundingScheme_HalfDown:
				p1.rounding = EN_RoundingScheme_HalfDown;
				break;
			case EN_RoundingScheme_AlwaysUp:
				p1.rounding = EN_RoundingScheme_AlwaysUp;
				break;
			case EN_RoundingScheme_AlwaysDown:
				p1.rounding = EN_RoundingScheme_AlwaysDown;
				break;
			case EN_RoundingScheme_Truncate:
				p1.rounding = EN_RoundingScheme_Truncate;
				break;
			case EN_RoundingScheme_Count:
				p1.rounding = EN_RoundingScheme_Count;
				break;
			}
			p1.firmId = result.Data[i][6];

			//New Data Retrieved
			switch (std::stoi(result.Data[i][7]))
			{
			case EN_FeeCategoryType_TradeFee:
				p1.feeTypeId = EN_FeeCategoryType_TradeFee;
				break;
			case EN_FeeCategoryType_MpidFee:
				p1.feeTypeId = EN_FeeCategoryType_MpidFee;
				break;
			default:
				p1.feeTypeId = EN_FeeCategoryType_None;
			}

			p1.parentPlanId = result.Data[i][8];

			plans.emplace_back(p1);
		}
	}
	DEBUG_LOG("FeeModule", fmt::format("{} - Plans Retrieved From Database", __PRETTY_FUNCTION__));
	return 0;
}

int DbDataAdapter::RetrieveRelevantTradeFee(std::vector<TradeFee>& tradeFees, const Adjustment& adjustment)
{
	std::string Ids = "'" + adjustment.accountId + "','" + adjustment.mpidId + "'";
	std::string select = "SELECT T.\"Id\", T.\"PlanId\", T.\"AccessId\", T.\"AccessType\" FROM \"TradeFees\" T LEFT JOIN \"TradeFeeDetails\" D on T.\"Id\" = D.\"TradeFeeId\" ";
	std::string whereCheck = "WHERE \"AccessId\" in (" + Ids + ") AND \"IsActive\" = true AND \"FeeCategoryType\" in (1,3) AND ";
	std::string dateCheck = "CAST(D.\"StartDate\" as Date) <= '" + adjustment.date + "' and (CAST(D.\"EndDate\" as Date) >= '" + adjustment.date + "' or D.\"EndDate\" is null);\n\0";

	std::string query = select + whereCheck + dateCheck;

	QueryResult result;
	int err = fetchDataFromStaticDb(query, result);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		return 1;
	}
	else
	{
		for (int i = 0; i < result.Data.size(); i++)
		{
			TradeFee t1;
			t1.id = result.Data[i][0];
			t1.planId = result.Data[i][1];
			t1.accessId = result.Data[i][2];

			switch (std::stoi(result.Data[i][3]))
			{
			case EN_AccessType_Account:
				t1.accessType = EN_AccessType_Account;
				t1.accountId = t1.accessId;
				break;
			case EN_AccessType_Firm:
				t1.accessType = EN_AccessType_Firm;
				t1.firmId = t1.accessId;
				break;
			case EN_AccessType_MPID:
				t1.accessType = EN_AccessType_MPID;
				t1.mpidId = t1.accessId;
				break;
			default:
				t1.accessType = EN_AccessType_None;
				//WARNING_LOG("FeeModule", fmt::format("{} - Access Type is None for TradeFeesId: {}", __PRETTY_FUNCTION__, t1.id));
			}
			tradeFees.emplace_back(t1);
		}
	}

	DEBUG_LOG("FeeModule", fmt::format("{} - Trade Fees Retrieved From Database", __PRETTY_FUNCTION__));
	return 0;
}

int DbDataAdapter::RetrieveMpidExecutedOrders(std::vector<std::vector<std::string>>& ret, const std::string& mpidIds, const std::string& date)
{
	//IMPORTANT: THIS FUNCTION IS FOR ENGINE SPECIFIC HENCE THE QUERY IS SUPPOSED TO WORK ON POSTGRES ONLY
	std::int64_t offset = 0;
	const int limit = 10000;
	ret.reserve(100000);

	while (true)
	{
		std::string columns =
			"\"OrderId\", \"ExecutionId\", \"Time\", \"Date\", \"AccountValue\", \"Route\", "
			"\"Penny\", \"LIQ\", \"Price\", \"Quantity\", \"ExecBroker\", \"Contra\", "
			"\"InternalRoute\", \"Tape\", \"MonthlyVolume\", \"Type\", \"AfterHours\", "
			"\"InternalLiq\", \"Side\", \"Symbol\", \"ExecQty\", \"AvgPx\", \"LastShares\", "
			"\"LastPx\", \"Capacity\", \"FillCount\", \"BeforeHours\", \"Lot\", \"DFID\", "
			"\"MPIDRecv\", \"FileName\", \"ExecTransType\", \"ExecutionDate\", "
			"\"MPIDMonthlyVolume\", \"FirmMonthlyVolume\", \"LastMarket\", \"FirmId\"";

		
		/*std::string accounts =
			"ARRAY(SELECT \"Value\" FROM \"Accounts\" WHERE \"MPIDId\" IN (" + mpidIds + "))";*/
		std::string accounts =
			"ARRAY(SELECT \"Value\" FROM \"Accounts\" WHERE \"MPIDId\" = ANY(ARRAY[" + mpidIds + "]))";

		std::string query =
			"SELECT " + columns +
			" FROM \"ExecutionData\" WHERE \"ExecutionDate\" >= '" + date + "'::date "
			"AND \"ExecutionDate\" < ('" + date + "'::date + INTERVAL '1 day') "
			"AND \"AccountValue\" = ANY(" + accounts + ") "
			"ORDER BY \"OrderId\", \"ExecutionId\", \"ExecutionDate\" "
			"LIMIT " + std::to_string(limit) +
			" OFFSET " + std::to_string(offset) + ";";



		offset += limit;

		QueryResult result;
		
		int err = db_inserter.fetchQuery(query, result);
		if (err == 1)
		{
			//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
			//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
			return 1;
		}
		for (const auto& data : result.Data)
		{
			ret.emplace_back(data);
		}

		if (result.Data.size() < limit)
		{
			break;
		}
	}
	//INFO_LOG("FeeModule", fmt::format("{} - {} Executions Data Retrieved From Database of MPID:{}", __PRETTY_FUNCTION__, ret.size(), mpidIds));
	return 0;
}


int DbDataAdapter::RetrieveAccountExecutedOrders(std::vector<std::vector<std::string>>& ret, const std::string& accountValues, const std::string& date)
{
	//IMPORTANT: THIS FUNCTION IS FOR ENGINE SPECIFIC HENCE THE QUERY IS SUPPOSED TO WORK ON POSTGRES ONLY
	std::int64_t offset = 0;
	const int limit = 10000;
	ret.reserve(100000);
	
	while (true)
	{
		std::string columns = "\"OrderId\", \"ExecutionId\", \"Time\", \"Date\", \"AccountValue\", \"Route\", \"Penny\", \"LIQ\", \"Price\", \"Quantity\", \"ExecBroker\", \"Contra\", \"InternalRoute\", \"Tape\", \"MonthlyVolume\", \"Type\", \"AfterHours\", \"InternalLiq\", \"Side\", \"Symbol\", \"ExecQty\", \"AvgPx\", \"LastShares\", \"LastPx\", \"Capacity\", \"FillCount\", \"BeforeHours\", \"Lot\", \"DFID\", \"MPIDRecv\", \"FileName\", \"ExecTransType\",\"ExecutionDate\", \"MPIDMonthlyVolume\", \"FirmMonthlyVolume\", \"LastMarket\" , \"FirmId\",\"TradeEntryType\",\"TradeDate\",\"ParentId\",\"ParentValue\" ";

		std::string query = "SELECT " + columns + " FROM \"ExecutionData\" WHERE \"ExecutionDate\" >= '" + date + "'::date "
			"AND \"ExecutionDate\" < ('" + date + "'::date + INTERVAL '1 day') "
			"AND (\"AccountValue\" = ANY(ARRAY[" + accountValues + "]) "
			"OR \"AccountId\" = ANY(ARRAY[" + accountValues + "])) "
			"ORDER BY \"OrderId\",\"ExecutionId\",\"Date\" "
			"LIMIT " + std::to_string(limit) + " OFFSET " + std::to_string(offset) + ";";
		

		offset += limit;

		QueryResult result;
		//auto err = databaseManager_dynamic.fetch_data(query, result);
		int err = db_inserter.fetchQuery(query, result);
		if (err == 1)
		{
			//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
			//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
			return 1;
		}
		for (const auto& data : result.Data)
		{
			ret.emplace_back(data);
		}

		if (result.Data.size() < limit)
		{
			break;
		}

	}
	//INFO_LOG("FeeModule", fmt::format("{} - {} Executions Data Retrieved From Database of AccountId:{}", __PRETTY_FUNCTION__, ret.size(), accountValues));

	return 0;
}

void DbDataAdapter::BulkExecuteOnlyMpid()
{
#ifdef BULK_INSERT
	//INFO_LOG("FeeModule", fmt::format("{} - BULK EXECUTION STARTED", __PRETTY_FUNCTION__));
	int err = 1;
	err = db_inserter.insertFromCSV(model.MPIDFeeBreakDownData, "MPIDFeeBreakDown", model.getColumns("MPIDFeeBreakDown"));
	if (err)
	{
		std::cerr << "MPIDFeeBreakDown: Bulk Insertion Failed\n";
		//WARNING_LOG("FeeModule", fmt::format("{} - MPIDFeeBreakDown: Bulk Insertion Failed", __PRETTY_FUNCTION__));
	}
	else
	{
		DEBUG_LOG("FeeModule", fmt::format("{} - MPIDFeeBreakDown Added To DB Successfully", __PRETTY_FUNCTION__));
	}

	//INFO_LOG("FeeModule", fmt::format("{} - Done Adding To Database", __PRETTY_FUNCTION__));
#endif
}


std::string DbDataAdapter::getMinDateOfTradeByAccount(const std::string& accountValues, const std::string& strDate, const std::string& endDate)
{
	//std::string dateCondition = "WHERE \"ExecutionDate\" >= '" + strDate + "' "
	//"AND \"ExecutionDate\" <= '" + endDate + "' ";
	//std::string accountValueCondition = "AND \"AccountValue\" = ANY(ARRAY[" + accountValues + "]) ";
	//std::string accountIdCondition = "AND \"AccountId\" = ANY(ARRAY[" + accountValues + "]) ";
	//std::string query =
	//	"SELECT MIN(\"ExecutionDate\"::date) "
	//	"FROM ("
	//	"SELECT \"ExecutionDate\" FROM \"ExecutionData\" " + dateCondition + accountValueCondition +
	//	"UNION ALL "
	//	"SELECT \"ExecutionDate\" FROM \"ExecutionData\" " + dateCondition + accountIdCondition +
	//	") t;";
	std::string query =
		"SELECT MIN(\"ExecutionDate\"::date) "
		"FROM \"ExecutionData\" "
		"WHERE \"ExecutionDate\" >= '" + strDate + "' "
		"AND \"ExecutionDate\" <= '" + endDate + "' "
		"AND (\"AccountValue\" = ANY(ARRAY[" + accountValues + "]) "
		"OR \"AccountId\" = ANY(ARRAY[" + accountValues + "]));";
	QueryResult result;
	//int err = databaseManager_dynamic.fetch_data(query, result);
	int err = db_inserter.fetchQuery(query, result);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		throw std::exception("Invalid AccountId Provided");
	}
	else
	{
		std::string retDate = { "" };
		if (!result.Data.empty() && !result.Data[0].empty()) {
			retDate = result.Data[0][0];
			if (retDate.size() >= 10)
				retDate = retDate.substr(0, 4) + retDate.substr(5, 2) + retDate.substr(8, 2);
		}

		if (retDate > strDate) {
			return retDate;
		}
		else if (retDate == strDate) {
			return strDate;
		}
		else {
			return std::string("");
		}
	}
}

std::string DbDataAdapter::getMinDateOfTradeByMPID(const std::string& mpidIds, const std::string& strDate, const std::string& endDate)
{
	
	std::string query =
		"SELECT MIN(ed.\"ExecutionDate\"::date) "
		"FROM \"ExecutionData\" ed "
		"JOIN \"Accounts\" a ON ed.\"AccountValue\" = a.\"Value\" "
		"WHERE ed.\"ExecutionDate\" >= '" + strDate + "' "
		"AND ed.\"ExecutionDate\" <= '" + endDate + "' "
		"AND a.\"MPIDId\" = ANY(ARRAY[" + mpidIds + "]);";


	QueryResult result;
	//int err = databaseManager_dynamic.fetch_data(query, result);
	int err = db_inserter.fetchQuery(query, result);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		throw std::exception("Invalid MPID ID Provided");
	}
	else
	{
		std::string retDate = { "" };
		if (!result.Data.empty() && !result.Data[0].empty()) {
			retDate = result.Data[0][0];
			if (retDate.size() >= 10)
				retDate = retDate.substr(0, 4) + retDate.substr(5, 2) + retDate.substr(8, 2);
		}
		
		if (retDate > strDate) {
			return retDate;
		}
		else if (retDate == strDate) {
			return strDate;
		}
		else {
			return std::string("");
		}
	}
}

std::string DbDataAdapter::getMinDateOfTradeByPlanId(const std::string& planIds, const std::string& strDate, const std::string& endDate)
{
	std::vector<Account> accounts;
	AdjustmentType type{};
	RetrievePlanAccounts(accounts, planIds, type);

	std::string query;
	std::string accountValues;
	QueryResult result;

	if (accounts.size() > 0)
	{
		for (auto itr = accounts.begin(); itr != std::prev(accounts.end()); ++itr)
		{
			accountValues += "'" + itr->name + "',";
		}
		accountValues += "'" + std::prev(accounts.end())->name + "'";

		std::string accCondition = " \"AccountValue\" IN (" + accountValues + ") AND ";
		std::string dateCondition = "\"ExecutionDate\" >= '" + strDate + "' "
		"AND \"ExecutionDate\" <= '" + endDate + "'; \n";
		query = "SELECT MIN(\"ExecutionDate\" As Date) FROM \"ExecutionData\" WHERE " + accCondition + dateCondition;
	}
	else
	{
		return "";
	}

	//int err = databaseManager_dynamic.fetch_data(query, result);
	int err = db_inserter.fetchQuery(query, result);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		throw std::exception("Invalid PlanId Provided");
	}

	std::string retDate = result.Data.at(0).at(0);
	return (result.Data.size() > 0 ? retDate : std::string(""));
}

std::string DbDataAdapter::getMpidOfAccount(const std::string& accountValue)
{

	std::string query = "SELECT \"MPIDId\" FROM \"Accounts\" WHERE \"Value\" = '"
		+ accountValue + "' OR \"Id\" = '"
		+ accountValue + "';";
	QueryResult result;
	int err = fetchDataFromStaticDb(query, result);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		throw std::runtime_error("Invalid MPID ID Provided");
	}
	if (result.Data.empty()) {
		return ""; 
	}

	if (result.Data[0].empty()) {
		return "";
	}
	return result.Data[0][0];
}

int DbDataAdapter::dataRetrieveFirms(std::vector<std::vector<std::string>>& firms)
{
	std::string query = "SELECT \"DFID\", \"Id\" FROM \"Firms\" WHERE \"IsActive\" = True AND \"IsDeleted\" = false;\n";
	QueryResult result;
	int err = fetchDataFromStaticDb(query, result);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		return 1;
	}
	else
	{
		firms = result.Data;
	}
	return 0;
}

int DbDataAdapter::dataRetrieveMpids(std::vector<std::vector<std::string>>& mpids)
{
	std::string query = "SELECT \"Value\", \"Id\" FROM \"MPID\" WHERE \"IsActive\" = True AND \"IsDeleted\" = false;\n";
	QueryResult result;
	int err = fetchDataFromStaticDb(query, result);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		return 1;
	}
	else
	{
		mpids = result.Data;
	}
	return 0;
}

int DbDataAdapter::dataRetrievePlanType(std::vector<std::vector<std::string>>& planAndType)
{
	std::string query = "SELECT \"Id\", \"PlanType\" FROM \"Plans\" WHERE \"IsActive\" = True AND \"IsDeleted\" = false;\n";
	QueryResult result;
	int err = fetchDataFromStaticDb(query, result);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		return 1;
	}
	else
	{
		planAndType = result.Data;
	}
	return 0;
}

int DbDataAdapter::dataRetrieveTradeOrderFees(std::vector<std::vector<std::string>>& orderFees)
{
	std::string query = "SELECT \"OrderId\", \"PlanId\", \"OrderFee\" FROM \"TradeFeeBreakDown\" tfbd WHERE \"ExecutionId\" = (SELECT MAX(\"ExecutionId\") FROM \"TradeFeeBreakDown\" tf WHERE tf.\"OrderId\" = tfbd.\"OrderId\")";
	QueryResult result;
	//int err = databaseManager_dynamic.fetch_data(query, result);
	int err = db_inserter.fetchQuery(query, result);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		return 1;
	}
	else
	{
		orderFees = result.Data;
	}
	return 0;
}

int DbDataAdapter::dataRetrieveMPIDOrderFees(std::vector<std::vector<std::string>>& orderFees)
{
	std::string query = "SELECT \"OrderId\", \"PlanId\", \"OrderFee\" FROM \"MPIDFeeBreakDown\" mfbd WHERE \"ExecutionId\" = (SELECT MAX(\"ExecutionId\") FROM \"MPIDFeeBreakDown\" mf WHERE mf.\"OrderId\" = mfbd.\"OrderId\")";
	QueryResult result;
	//int err = databaseManager_dynamic.fetch_data(query, result);
	int err = db_inserter.fetchQuery(query, result);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		return 1;
	}
	else
	{
		orderFees = result.Data;
	}
	return 0;
}

void DbDataAdapter::dataRetrieveOrderFees(std::vector<std::vector<std::string>>& orderFees)
{
	const int reserve = 100000;
	std::vector<std::vector<std::string>> tradeFees, mpidFees;

	tradeFees.reserve(reserve);
	mpidFees.reserve(reserve);

	dataRetrieveTradeOrderFees(tradeFees);
	dataRetrieveMPIDOrderFees(mpidFees);

	orderFees.reserve(tradeFees.size() + mpidFees.size());

	orderFees.insert(orderFees.end(), tradeFees.begin(), tradeFees.end());
	orderFees.insert(orderFees.end(), mpidFees.begin(), mpidFees.end());
}
inline std::string trim(const std::string& s) {
	auto start = s.find_first_not_of(" \t\n\r");
	auto end = s.find_last_not_of(" \t\n\r");
	return (start == std::string::npos) ? "" : s.substr(start, end - start + 1);
}
int DbDataAdapter::GetMpidID(Adjustment& adjustment) {
	std::string query = "SELECT \"Id\" FROM \"MPID\" WHERE \"Value\" = '" + adjustment.mpidId + "';";
	QueryResult result;
	//int err = databaseManager_dynamic.fetch_data(query, result);
	int err = db_inserter.fetchQuery(query, result);
	if (err == 1)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'DB Query Failed'";
		//LogWarning() << "Query: " << query;
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		return 1;
	}
	else {
		adjustment.mpidId = result.Data[0][0];
		return 0;
	}
}
int DbDataAdapter::getEventDataById(const std::string& eventId, std::string& json)
{
	////INFO_LOG("DatabaseLibrary", fmt::format("{} - eventId (quoted): {}", __PRETTY_FUNCTION__, std::quoted(eventId)));
	std::string cleanId = trim(eventId);
	std::string query = "SELECT \"Payload\" FROM \"FCMEventData\" WHERE \"Id\" = '" + cleanId + "';";
	//"SELECT \"Id\", \"PlanType\" FROM \"Plans\" WHERE \"IsActive\" = True AND \"IsDeleted\" = false;\n";
	QueryResult result;
	//int err = databaseManager_dynamic.fetch_data(query, result);
	int err = db_inserter.fetchQuery(query, result);
	if (err == 1)
	{
		//WARNING_LOG("FeeModule", fmt::format("{} - DB Query Failed", __PRETTY_FUNCTION__));
		//WARNING_LOG("FeeModule", fmt::format("{} - Query: {}", __PRETTY_FUNCTION__, query));
		return 1;
	}
	else
	{
		json = result.Data.size() > 0 ? result.Data.at(0).at(0) : "";
		return 0;
	}
}
