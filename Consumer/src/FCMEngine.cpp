#include "../FeeModule/FCMEngine.h"
#include <sstream>
#include <boost/date_time/gregorian/gregorian.hpp>
//#include "../FeeModule/logger/logger.hpp"
#include "../FeeModule/DbDataAdapter.h"
#include <regex>

std::string FCM_Engine::getDbName(const std::string& input)
{
	// Find the position of "Database="
	size_t startPos = input.find("Database=");

	if (startPos != std::string::npos) {
		// Find the position of the next semicolon after "Database="
		size_t endPos = input.find(";", startPos);

		if (endPos != std::string::npos) {
			// Extract the substring between "Database=" and the semicolon
			return input.substr(startPos, endPos - startPos);
		}
	}

	// Return an empty string if "Database=" is not found or there's no semicolon after it.
	return "";
}
std::string FCM_Engine::filePath = "Report.PropFilesDirectory";
bool FCM_Engine::isDelete = true;

std::string FCM_Engine::getCurrentDate()
{
	boost::gregorian::date date = boost::gregorian::day_clock::local_day(); // Format the date as YYYYMMDD
	std::ostringstream oss;
	oss << boost::gregorian::to_iso_string(date);
	return oss.str();
}


std::string FCM_Engine::convertDateFormat(const std::string& date)
{
	std::string year = date.substr(0, 4);
	std::string month = date.substr(4, 2);
	std::string day = date.substr(6, 2);
	std::string result = year + "-" + month + "-" + day;

	return result;
}



FCM_Engine::FCM_Engine(const std::string& config_name, bool logstatus, const std::string& date) :
	setting(config_name),
	fee_module(config_name, convertDateFormat(date)),
	logflag(logstatus),
	reportManager(Report, setting.Get<std::string>(FCM_Engine::filePath),
		SymbolData, setting.Get<std::string>("Report.TestSymbolsFile"))
{
	//// LogInfo() << "RUNNING FOR DATE: "<< date;
	//INFO_LOG("FCM-Engine", fmt::format("{} RUNNING FOR DATE{}", __PRETTY_FUNCTION__, date));
	//// LogInfo() << "DB STATIC: " << getDbName(setting.Get<std::string>("Database.ConnectionStringStatic"));
	//INFO_LOG("FCM-Engine", fmt::format("{} DB STATIC:{}", __PRETTY_FUNCTION__, getDbName(setting.Get<std::string>("Database.ConnectionStringStatic"))));
	//// LogInfo() << "DB TRANSACT: " << getDbName(setting.Get<std::string>("Database.ConnectionStringTransact"));
	//INFO_LOG("FCM-Engine", fmt::format("{} DB TRANSACT:{}", __PRETTY_FUNCTION__, getDbName(setting.Get<std::string>("Database.ConnectionStringTransact"))));
}

void FCM_Engine::setDebugFlag(bool flag)
{
	logflag = flag;
}

void FCM_Engine::calculateFees(std::priority_queue<OrderExecutionData>& ordersData)
{
	int completed = 0;
	int total = ordersData.size();
	//for (const auto& order : ordersData)
	while (!ordersData.empty())
	{
		auto order = ordersData.top();
		ordersData.pop();
		//FeeAndCommission
		auto fee = fee_module.CalculateFeeAndComission(order); //curr.second
		completed++;
		if (completed % 5000 == 0)
		{
			INFO_LOG("FCM-Engine", fmt::format("{} - {}/{} executions completed", __PRETTY_FUNCTION__, completed, total));
		}
	}

	if (logflag)
		std::cout << "Total Executions Counter: " << completed << std::endl;
	INFO_LOG("FCM-Engine", fmt::format("{} - Total Executions Counter {}", __PRETTY_FUNCTION__, completed));

}
void FCM_Engine::calculateFees(std::priority_queue<OrderExecutionData>& ordersData, Adjustment& adjustment)
{
	int completed = 0;
	int total = ordersData.size();
	//for (const auto& order : ordersData)
	while (!ordersData.empty())
	{
		auto order = ordersData.top();
		ordersData.pop();
		//FeeAndCommission
		auto fee = fee_module.CalculateFeeAndComission(order, adjustment); //curr.second
		
		completed++;
		if (completed % 5000 == 0)
		{
			INFO_LOG("FCM-Engine", fmt::format("{} - {}/{} executions completed", __PRETTY_FUNCTION__, completed, total));
		}
	}

	if (logflag)
		std::cout << "Total Executions Counter: " << completed << std::endl;
	INFO_LOG("FCM-Engine", fmt::format("{} - Total Executions Counter {}", __PRETTY_FUNCTION__, completed));

}
void FCM_Engine::init(const std::string& date,const std::optional<std::string>& cdnFilePath)
{

	try
	{
		fee_module.init();
	}
	catch (const std::exception& ex)
	{
		WARNING_LOG("FCM-Engine",
			fmt::format("{} - {}", __PRETTY_FUNCTION__, ex.what()));
		throw std::runtime_error("Engine Failed To Initialize");
	}

	try
	{
		auto worker_mem = setting.Get<std::string>("Database.workerSize", "4MB");

		dataManager.setWorkerMem(worker_mem);

		dataManager.RetrieveAccountsData(fee_module.getAccounts());

		reportManager.ClearReports();
		reportManager.GetSymbolsDataFromFile(date);
		dataManager.ProcessSymbolData(SymbolData);

		Report.reserve(3000000);


		if (cdnFilePath.has_value())
		{
			// Parse CDN files
			reportManager.ParseAllFilesCDN(date, cdnFilePath.value());
		}
		else
		{
			// Parse regular EOD files
			reportManager.ParseEODFiles(date);
		}

		if (!Report.empty())
		{
			dataManager.RetrieveExecutedOrderData(Report, date);
		}
	}
	catch (const std::exception& ex)
	{
		WARNING_LOG("FCM-Engine", fmt::format("{} - ERROR OCCURED: {}", __PRETTY_FUNCTION__, ex.what()));

		throw std::runtime_error(fmt::format("Engine Failed Load Symbol OR Prop Files Properly - {}", ex.what()));
	}
}


void FCM_Engine::finish()
{
	fee_module.finish();
}

//================================== BACK DATE ========================================

void FCM_Engine::init(Adjustment& adjustment)
{
	try
	{
		if (adjustment.adjustmentType == EN_Adjustment_Drop_Copy) {
			fee_module.init();
		}
		else
		{
			fee_module.init(adjustment); // MOVE IT TO INIT FUNCTION
		}
		
	}
	catch (const std::exception &e)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << e.what();
		WARNING_LOG("FCM-Engine", fmt::format("{} - {} ", __PRETTY_FUNCTION__, e.what()));
	}
	catch (...)
	{
		//LogWarning() << __PRETTY_FUNCTION__
		//	<< " 'Ohh Shoot... Some Unknown Error Occured...";
		WARNING_LOG("FCM-Engine", fmt::format("{} - Ohh Shoot... Some Unknown Error Occured...", __PRETTY_FUNCTION__));
	}
	

	
	
	if (adjustment.adjustmentType == EN_Adjustment_Drop_Copy) {
		auto worker_mem = setting.Get<std::string>("Database.workerSize", "4MB");
		dataManager.setWorkerMem(worker_mem);
		dataManager.RetrieveAccountsData();
		reportManager.ClearReports();	
		reportManager.GetSymbolsDataFromFile(adjustment.date);
		dataManager.ProcessSymbolData(SymbolData);
		Report.reserve(3000000);
		reportManager.ParseDropFiles(adjustment.accessIds);
		//if (!Report.empty())
		//{
		//	//dataManager.RetrieveExecutedOrderData(Report,adjustment.date);
		//}

	}
	else
	{
		dataManager.RetrieveRelevantAccountsData(adjustment);
		reportManager.GetSymbolsDataFromFile(adjustment.date);
		dataManager.ProcessSymbolData(SymbolData);
	}
	
}

bool isGUID(const std::string& str)
{
	// Define a regular expression pattern that matches either a GUID or an alphanumeric account number
	std::regex pattern("^[{]?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}[}]?$");
	//std::regex pattern("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$|^[0-9A-Za-z\\-]+$");
	return std::regex_match(str, pattern);
}

int FCM_Engine::RunBackDateAccounts(Adjustment& adjustment)
{
	int err = 1;
	std::string accountValues{ "" };
	 
	try
	{
		fee_module.init();
	}
	catch (const std::exception& ex)
	{
		WARNING_LOG("FCM-Engine",
			fmt::format("{} - {}", __PRETTY_FUNCTION__, ex.what()));
		throw std::runtime_error("Engine Failed To Initialize");
	}
	try
	{

		reportManager.GetSymbolsDataFromFile(adjustment.date);
		dataManager.ProcessSymbolData(SymbolData);

	}
	catch (const std::exception& ex)
	{
		WARNING_LOG("FCM-Engine", fmt::format("{} - ERROR OCCURED: {}", __PRETTY_FUNCTION__, ex.what()));

		throw std::runtime_error(fmt::format("Engine Failed Load Symbol OR Prop Files Properly - {}", ex.what()));
	}
	adjustment.getStringifiedAccessIds(accountValues);

	

	reportManager.ClearReports();	//Cleaning Reports To Delete Garbage Data
	
	dataManager.RetrieveAccountDataFromDatabase(Report, accountValues, adjustment.date);

	//Generate Order Execution Data From the Data
	std::priority_queue<OrderExecutionData> orderData;
	dataManager.MakeOrdersFromDb(Report, orderData);
	reportManager.ClearReports();	//Cleaning Reports to save memory

	//Calculate Fees And Commission & Populate The Tables
	calculateFees(orderData, adjustment);

	//Delete all existing trasact data of that date/or by date of that account
	dataManager.CleanAccountTransactDbData(adjustment.date, accountValues);
	//dataManager.BulkExecute();
	dataManager.BulkExecuteWorkFlow();

	finish();
	err = 0;

	return err;
}

int FCM_Engine::RunBackDateOnlyMPIDs(Adjustment& adjustment)
{
	int err = 1;

	std::string mpidIds{ "" };

	if (adjustment.adjustmentType == EN_Adjustment_MPIDFee_Mpid)
	{
		init(adjustment); // Calling Specific MPID Based Initialization
		mpidIds = "'" + adjustment.mpidId + "'";
	}
	else
	{
		init(adjustment.date);
		adjustment.getStringifiedAccessIds(mpidIds);
	}

	reportManager.ClearReports();	//Cleaning Reports To Delete Garbage Data
	
	dataManager.RetrieveMpidDataFromDatabase(Report, mpidIds, adjustment.date);

	//Generate Order Execution Data From the Data
	std::priority_queue<OrderExecutionData> orderData;
	dataManager.MakeOrdersFromDb(Report, orderData);
	reportManager.ClearReports();	//Cleaning Reports to save memory

	//Calculate Fees And Commission & Populate The Tables
	calculateFees(orderData, adjustment);

	//Delete all existing trasact data of that date/or by date of that account
	dataManager.CleanMpidTransactDbData(adjustment.date, mpidIds);
	dataManager.BulkExecuteOnlyMpid();

	finish();
	err = 0;

	return err;
}

int FCM_Engine::RunBackDatePlans(Adjustment& adjustment)
{
	int err = 1;
	std::string accessIds = "";
	adjustment.getStringifiedAccessIds(accessIds);

	adjustment.accessIds.clear();
	std::vector<Account> accounts;
	DbDataAdapter::getInstance()->RetrievePlanAccounts(accounts, accessIds, EN_Adjustment_TradeFee_MultpleFirms);

	for (const auto& account : accounts)
	{
		adjustment.accessIds.push_back(account.name); //Name is basically accountValue
	}
	accounts.clear();

	adjustment.adjustmentType = EN_Adjustment_TradeFee_MultipleAccounts;

	if (adjustment.accessIds.size())
	{
		return RunBackDateAccounts(adjustment);
	}
	else
	{
		//LogInfo() << __PRETTY_FUNCTION__ << "No Accounts Found To Run Against";
		INFO_LOG("FCM-Engine", fmt::format("{} - No Accounts Found To Run Against", __PRETTY_FUNCTION__));
		return 0;
	}
}

int FCM_Engine::RunBackDateFirms(Adjustment& adjustment)
{
	int err = 1;
	std::string accessIds = "";
	adjustment.getStringifiedAccessIds(accessIds);

	adjustment.accessIds.clear();
	std::vector<Account> accounts;
	DbDataAdapter::getInstance()->RetrieveFirmAccounts(accounts, accessIds, EN_Adjustment_TradeFee_MultpleFirms);
	//Shift This To StaticDataManager ^

	for (const auto& account : accounts)
	{
		adjustment.accessIds.push_back(account.name); //Name is basically accountValue
	}
	accounts.clear();

	adjustment.adjustmentType = EN_Adjustment_TradeFee_MultipleAccounts;

	if (adjustment.accessIds.size())
	{
		return RunBackDateAccounts(adjustment);
	}
	else
	{
		//LogInfo() << __PRETTY_FUNCTION__ << "No Accounts Found To Run Against";
		INFO_LOG("FCM-Engine", fmt::format("{} - No Accounts Found To Run Against", __PRETTY_FUNCTION__));
		return 0;
	}
}

int FCM_Engine::RunBackDateMpids(Adjustment& adjustment)
{
	int err = 1;
	std::string accessIds = "";
	adjustment.getStringifiedAccessIds(accessIds);

	adjustment.accessIds.clear();
	std::vector<Account> accounts;
	DbDataAdapter::getInstance()->RetrieveMpidAccounts(accounts, accessIds, EN_Adjustment_TradeFee_MultipleMpids);
	//Shift This To StaticDataManager ^

	for (const auto& account : accounts)
	{
		adjustment.accessIds.push_back(account.name); //Name is basically accountValue
	}
	accounts.clear();

	adjustment.adjustmentType = EN_Adjustment_TradeFee_MultipleAccounts;

	if (adjustment.accessIds.size())
	{
		return RunBackDateAccounts(adjustment);
	}
	else
	{
		//LogInfo() << __PRETTY_FUNCTION__ << "No Accounts Found To Run Against";
		INFO_LOG("FCM-Engine", fmt::format("{} - No Accounts Found To Run Against", __PRETTY_FUNCTION__));
		return 0;
	}
}

int FCM_Engine::RunBackDate(Adjustment& adjustment)
{
	int err = 1;
	try
	{
		if (adjustment.workFlowType == EN_WorkFlowType_BackDate)
		{
			switch (adjustment.adjustmentType)
			{
			case EN_Adjustment_TradeFee_Account:
			case EN_Adjustment_TradeFee_MultipleAccounts:
				err = RunBackDateAccounts(adjustment);
				break;
			case EN_Adjustment_TradeFee_Mpid:
			case EN_Adjustment_TradeFee_MultipleMpids:
				err = RunBackDateMpids(adjustment);
				break;

			case EN_Adjustment_TradeFee_Firm:
			case EN_Adjustment_TradeFee_MultpleFirms:
				err = RunBackDateFirms(adjustment);
				break;

			case EN_Adjustment_MPIDFee_Mpid:
			case EN_Adjustment_MPIDFee_MultipleMpids:
				err = RunBackDateOnlyMPIDs(adjustment);
				break;

			case EN_Adjustment_TradeFee_Plan:
				err = RunBackDatePlans(adjustment);
				break;
			case EN_Adjustment_Add_Execution:
				err = IngestManualAccount(adjustment);
				break;
			default:
				throw std::exception("Invalid Adjustment Type");
			}
		}
		else
		{
			throw std::exception("Invalid Work Flow Type");
		}
	}
	catch (const std::exception &e)
	{
		finish();
		//LogWarning() << __PRETTY_FUNCTION__ << e.what();
		WARNING_LOG("FCM-Engine", fmt::format("{} - {} ", __PRETTY_FUNCTION__, e.what()));
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Failure: Failed For " << adjustment.date << "'";
		WARNING_LOG("FCM-Engine", fmt::format("{} - Failure: Failed For: {} ' ", __PRETTY_FUNCTION__, adjustment.date));
		throw std::exception(e.what());
	}
	catch (...)
	{
		finish();
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Failure: Failed For " << adjustment.date << "'";
		WARNING_LOG("FCM-Engine", fmt::format("{} - Failure: Failed For: {} ' ", __PRETTY_FUNCTION__, adjustment.date));
		throw std::exception("Failed - Unknown Exception Encountered");
	}
	return err;
}

//=============================================================================================================================================
void FCM_Engine::getAccountData( std::vector<std::vector<std::string>>& accountValues, const std::string& date, const std::string& filePath_CDN)
{
	const std::string directoryPath = filePath_CDN; //previouslly a hard coded path was used here to parse CDN Files
    const std::string fileName = date + ".csv"; 

    // Construct full path to the file
    std::filesystem::path filePath = std::filesystem::path(directoryPath) / fileName;

    // Check if the file exists
    if (!std::filesystem::exists(filePath)) {
        std::cerr << "File not found: " << filePath << std::endl;
        return; // Exit if file does not exist
    }

    std::ifstream file(filePath);
    if (!file.is_open()) {
        std::cerr << "Failed to open the file: " << filePath << std::endl;
        return; // Exit if file cannot be opened
    }

    std::string line;
    bool isHeader = true;
    std::vector<std::string> headers;

    // Read each line from the CSV
    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string value;
        
        // Read header row to identify column indices
        if (isHeader) {
            while (std::getline(ss, value, ',')) {
                headers.push_back(value);
            }
            isHeader = false;
            continue; // Skip header row
        }

        // Prepare a vector to hold values for this row
        std::vector<std::string> rowValues;

        // Reset stringstream for this line
        ss.clear();
        ss.str(line);

        size_t columnIndex = 0;
        while (std::getline(ss, value, ',')) {
            // Insert values based on column names
            if (headers[columnIndex] == "Account") {
                rowValues.push_back(value);
				
            } else if (headers[columnIndex] == "Fill Id") {
                rowValues.push_back(value);
            } else if (headers[columnIndex] == "Order Id") {
                rowValues.push_back(value);
            }
            columnIndex++;
        }

        // Add the row vector to accountValues if it contains data
        if (!rowValues.empty()) {
            accountValues.push_back(rowValues);
        }
    }

    file.close(); // Close the file after reading
}
int FCM_Engine::IngestDateWithCDN(Adjustment& adjustment,std::string& filePath_cdn) 
{
	int err = 1;
	try
	{
		init(adjustment.date, filePath_cdn);

		// Create the resolver once — binds to fee_module's method
		auto dateResolver = [this](const std::string& date, const std::string& time) {
			return fee_module.getEffectiveBusinessDate(date, time);
		};

		std::priority_queue<OrderExecutionData> orderData;
		dataManager.MakeOrders(Report, orderData, adjustment, dateResolver);

		
		reportManager.ClearReports();

		
		calculateFees(orderData);
		if (FCM_Engine::isDelete)
		{
			std::vector<std::vector<std::string>> candidateKeysVector;
			getAccountData(candidateKeysVector, adjustment.date, filePath_cdn);//This function particularly populates account, Fill Id and Order Id from CDN Files
			for (auto& candidateKeys : candidateKeysVector)
			{
				adjustment.accessIds.clear();
				adjustment.accessIds.push_back(candidateKeys[0]);
				std::string accountValuesStringified = "";
				adjustment.getStringifiedAccessIds(accountValuesStringified);
				adjustment.accessIds.clear();
				dataManager.CleanAccountTransactDbData(adjustment.date, accountValuesStringified, candidateKeys[2], candidateKeys[1]);
			}
		}
		else
		{
			std::cout << "Data Already Exists" << std::endl;
			INFO_LOG("FCM-Engine", fmt::format("{} - Data Already Exists", __PRETTY_FUNCTION__));
		}
	//	dataManager.BulkExecuteWorkFlow(); //This method uses insert on conflict on ExecutionData because in normal flows(except EOD and Drop), if data is deleted then it cannot be regenerated
		dataManager.BulkExecute();
		finish();
		err = 0;
	}
	catch (const std::exception& e)
	{
		WARNING_LOG("FCM-Engine", fmt::format("{} - {} ", __PRETTY_FUNCTION__, e.what()));
		err = 1;
		throw std::exception(e.what());
	}
	catch (...)
	{
		err = 1;
		finish();
		std::cerr << "Failure: Failed For " << adjustment.date << std::endl;
		WARNING_LOG("FCM-Engine", fmt::format("{} - Failure: Failed For: {} ' ", __PRETTY_FUNCTION__, adjustment.date));
	}
	return err;
}
int FCM_Engine::IngestDate(Adjustment& adjustment)
{
	int err = 1;
	try
	{
		init(adjustment.date);

		 // Create the resolver once — binds to fee_module's method
		auto dateResolver = [this](const std::string& date, const std::string& time) {
			return fee_module.getEffectiveBusinessDate(date, time);
		};

		//Generate Order Execution Data From the Data
		std::priority_queue<OrderExecutionData> orderData;
		dataManager.MakeOrders(Report, orderData, adjustment, dateResolver);
		//Cleaning Reports to save memory
		reportManager.ClearReports();
		
		//Calculate Fees And Commission & Populate The Tables
		calculateFees(orderData);
		if (FCM_Engine::isDelete)
		{
			std::string accountValuesStringified = "";
			adjustment.getStringifiedAccessIds(accountValuesStringified);
			dataManager.CleanAccountTransactDbDataFileName(adjustment.date, accountValuesStringified, "FileName");
		}
		else
		{
			std::cout<<"Data Already Exists"<<std::endl;
			INFO_LOG("FCM-Engine", fmt::format("{} - Data Already Exists", __PRETTY_FUNCTION__));
		}
		dataManager.BulkExecute();
		finish();
		err = 0;
	}
	catch (const std::exception &e)
	{
		// no need to return value here and also no need to call finish becuase the throw will do its magic
		WARNING_LOG("FCM-Engine", fmt::format("{} - {} ", __PRETTY_FUNCTION__, e.what()));
		err = 1;
		throw std::exception(e.what());
	}
	catch (...)
	{
		err = 1;
		finish();
		std::cerr << "Failure: Failed For " <<adjustment.date<< std::endl;
		WARNING_LOG("FCM-Engine", fmt::format("{} - Failure: Failed For: {} ' ", __PRETTY_FUNCTION__, adjustment.date));
	}
	return err;
}

int FCM_Engine::IngestMultipleAccounts(Adjustment& adjustment)
{
	int err = 1;
	try
	{
		init(adjustment.date);

		std::unordered_set<std::string> accountValues;
		accountValues.insert(adjustment.accessIds.begin(), adjustment.accessIds.end());

		 // Create the resolver once — binds to fee_module's method
		auto dateResolver = [this](const std::string& date, const std::string& time) {
			return fee_module.getEffectiveBusinessDate(date, time);
		};

		std::priority_queue<OrderExecutionData> orderData;
		{
			std::priority_queue<OrderExecutionData> temp_orderData;
			dataManager.MakeOrders(Report, temp_orderData, adjustment, dateResolver);
			reportManager.ClearReports(); //Clearing Reports to Save Memory

			while (!temp_orderData.empty())//for (auto& data : temp_orderData)
			{
				auto data = temp_orderData.top();
				temp_orderData.pop();
				if (accountValues.count(data.accountValue) > 0)
				{
					orderData.emplace(data);	//Only Keeping the Order From Particular Accounts, Discarding the Rest
				}
			}
		}

		calculateFees(orderData);//Calculate Fees And Commission & Populate The Tables

		std::string accountValuesStringified = "";
		adjustment.getStringifiedAccessIds(accountValuesStringified);
		dataManager.CleanAccountTransactDbData(adjustment.date, accountValuesStringified);
		if (adjustment.adjustmentType == EN_Adjustment_Everything || adjustment.adjustmentType == EN_Adjustment_Drop_Copy)
		{
			dataManager.BulkExecute();
		}
		else
		{
			dataManager.BulkExecuteWorkFlow();
		}
		

		err = 0;
	}

	catch (const std::exception &e)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << e.what();
		WARNING_LOG("FCM-Engine", fmt::format("{} - {} ", __PRETTY_FUNCTION__, e.what()));
		err = 1;
	}

	catch (...)
	{
		std::cerr << "Failure: Failed For " << adjustment.date << std::endl;
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Failure: Failed For " << adjustment.date << "'";
		WARNING_LOG("FCM-Engine", fmt::format("{} - Failure: Failed For: {} ' ", __PRETTY_FUNCTION__, adjustment.date));
		err = 1;
	}

	finish();
	return err;
}
int FCM_Engine::IngestManualAccounts(Adjustment& adjustment)
{
	int err = 1;
	try
	{
		//init(adjustment.date);
		init(adjustment);
		std::unordered_set<std::string> accountValues;
		accountValues.insert(adjustment.accessIds.begin(), adjustment.accessIds.end());

		std::priority_queue<OrderExecutionData> orderData;
		{
			OrderExecutionData temp_orderData;
			
			dataManager.MakeManualOrder(temp_orderData, adjustment);
			
			
				auto data = temp_orderData;
				
				
					orderData.emplace(data);	
				
			
		}

		
		calculateFees(orderData);//Calculate Fees And Commission & Populate The Tables

		std::string accountValuesStringified = "";
		adjustment.getStringifiedAccessIds(accountValuesStringified);
		dataManager.CleanAccountTransactDbData(adjustment.date, accountValuesStringified, adjustment.ExecutionId,adjustment.OrderId);
		dataManager.BulkExecute();

		err = 0;
	}
	catch (const std::exception& e)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << e.what();
		WARNING_LOG("FCM-Engine", fmt::format("{} - {} ", __PRETTY_FUNCTION__, e.what()));
		err = 1;
	}

	catch (...)
	{
		std::cerr << "Failure: Failed For " << adjustment.date << std::endl;
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Failure: Failed For " << adjustment.date << "'";
		WARNING_LOG("FCM-Engine", fmt::format("{} - Failure: Failed For: {} ' ", __PRETTY_FUNCTION__, adjustment.date));
		err = 1;
	}

	finish();
	return err;
}

int FCM_Engine::IngestMultipleMPIDs(Adjustment& adjustment)
{
	std::string accessIds = "";
	adjustment.getStringifiedAccessIds(accessIds);

	std::vector<Account> accounts;
	DbDataAdapter::getInstance()->RetrieveMpidAccounts(accounts, accessIds, EN_Adjustment_TradeFee_MultipleMpids);
	//TO Do: Shift This To StaticDataManager

	for (const auto& account : accounts)
	{
		adjustment.accessIds.push_back(account.name); //Name is basically accountValue
	}
	accounts.clear();

	adjustment.adjustmentType = EN_Adjustment_TradeFee_MultipleAccounts;

	if (adjustment.accessIds.size())
	{
		return IngestMultipleAccounts(adjustment);
	}
	else
	{
		//LogInfo() << __PRETTY_FUNCTION__ << "No Accounts Found To Run Against";
		INFO_LOG("FCM-Engine", fmt::format("{} - No Accounts Found To Run Against", __PRETTY_FUNCTION__));
		return 0;
	}
}
int FCM_Engine::IngestDropCopy(Adjustment& adjustment) 
{
		init(adjustment);
		
		// Create the resolver once — binds to fee_module's method
		auto dateResolver = [this](const std::string& date, const std::string& time) {
			return fee_module.getEffectiveBusinessDate(date, time);
		};

		//Generate Order Execution Data From the Data
		std::priority_queue<OrderExecutionData> orderData;
		dataManager.MakeDropCopyOrders(Report, orderData, adjustment.date, adjustment.wingSpanSymbolData, dateResolver);
		reportManager.ClearReports();

		//Calculate Fees And Commission & Populate The Tables
		
		calculateFees(orderData);
		std::string accountValuesStringified = "";
		adjustment.getStringifiedAccessIds(accountValuesStringified);
		dataManager.CleanAccountTransactDbDataFileName(adjustment.date, accountValuesStringified, "FileName");
		dataManager.BulkExecute();

		finish();
		return 0;
		
}

int FCM_Engine::IngestMultipleFirms(Adjustment& adjustment)
{
	std::string accessIds = "";
	adjustment.getStringifiedAccessIds(accessIds);

	adjustment.accessIds.clear();
	std::vector<Account> accounts;
	DbDataAdapter::getInstance()->RetrieveFirmAccounts(accounts, accessIds, EN_Adjustment_TradeFee_MultpleFirms);
	//Shift This To StaticDataManager ^

	for (const auto& account : accounts)
	{
		adjustment.accessIds.push_back(account.name); //Name is basically accountValue
	}
	accounts.clear();

	adjustment.adjustmentType = EN_Adjustment_TradeFee_MultipleAccounts;

	if (adjustment.accessIds.size())
	{
		return IngestMultipleAccounts(adjustment);
	}
	else
	{
		//LogInfo() << __PRETTY_FUNCTION__ << "No Accounts Found To Run Against";
		INFO_LOG("FCM-Engine", fmt::format("{} - No Accounts Found To Run Against", __PRETTY_FUNCTION__));
		return 0;
	}
}
int FCM_Engine::IngestManualAccount(Adjustment& adjustment)
{
	if (adjustment.accessIds.size())
	{
		return IngestManualAccounts(adjustment);
	}
	else
	{
		//LogInfo() << __PRETTY_FUNCTION__ << "No Accounts Found To Run Against";
		INFO_LOG("FCM-Engine", fmt::format("{} - No Accounts Found To Run Against", __PRETTY_FUNCTION__));
		return 0;
	}
}

int FCM_Engine::Ingest(Adjustment& adjustment)
{
	int err = 1;
	try
	{
		if (adjustment.workFlowType == EN_WorkFlowType_Ingestion)
		{
			switch (adjustment.adjustmentType)
			{
			case EN_Adjustment_Everything:
				err = IngestDate(adjustment);
				break;
			case EN_Adjustment_TradeFee_Account:
			case EN_Adjustment_TradeFee_MultipleAccounts:
				err = IngestMultipleAccounts(adjustment);
				break;
			case EN_Adjustment_MPIDFee_Mpid:
			case EN_Adjustment_TradeFee_Mpid:
			case EN_Adjustment_MPIDFee_MultipleMpids:
				err = IngestMultipleMPIDs(adjustment);
				break;

			case EN_Adjustment_TradeFee_Firm:
			case EN_Adjustment_TradeFee_MultpleFirms:
				err = IngestMultipleFirms(adjustment);
				break;
			case EN_Adjustment_Drop_Copy:
				err = IngestDropCopy(adjustment);
				break;
			default:
				throw std::exception("Invalid Adjustment Type");
			}
		}
		else
		{
			throw std::exception("Invalid Work Flow Type");
		}
	}
	catch (const std::exception &e)
	{
		finish();
		//LogWarning() << __PRETTY_FUNCTION__ << e.what();
		WARNING_LOG("FCM-Engine", fmt::format("{} - {} ", __PRETTY_FUNCTION__, e.what()));
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Failure: Failed For " << adjustment.date << "'";
		WARNING_LOG("FCM-Engine", fmt::format("{} - Failure: Failed For: {} ' ", __PRETTY_FUNCTION__, adjustment.date));
		throw std::exception(e.what());
	}
	catch (...)
	{
		finish();
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Failure: Failed For " << adjustment.date << "'";
		WARNING_LOG("FCM-Engine", fmt::format("{} - Failure: Failed For: {} ' ", __PRETTY_FUNCTION__, adjustment.date));
		throw std::exception("Failed - Unknown Exception Encountered");
	}
	return err;
}
int FCM_Engine::RunCDN(Adjustment &adjustment,std::string& filePath)
{
	int err = 1;

	try
	{
		if (adjustment.workFlowType == EN_WorkFlowType_CDN)
		{
			switch (adjustment.adjustmentType)
			{
			case EN_Adjustment_Everything:
				err = IngestDateWithCDN(adjustment,filePath);
				break;
			default:
				throw std::exception("Invalid Adjustment Type");
			}
		}
		else
		{
			throw std::exception("Invalid Work Flow Type");
		}
	}
	catch (const std::exception &e)
	{
		finish();
		//LogWarning() << __PRETTY_FUNCTION__ << e.what();
		WARNING_LOG("FCM-Engine", fmt::format("{} - {} ", __PRETTY_FUNCTION__, e.what()));
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Failure: Failed For " << adjustment.date << "'";
		WARNING_LOG("FCM-Engine", fmt::format("{} - Failure: Failed For: {} ' ", __PRETTY_FUNCTION__, adjustment.date));
		throw std::exception(e.what());
	}
	catch (...)
	{
		finish();
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Failure: Failed For " << adjustment.date << "'";
		WARNING_LOG("FCM-Engine", fmt::format("{} - Failure: Failed For: {} ' ", __PRETTY_FUNCTION__, adjustment.date));
		throw std::exception("Failed - Unknown Exception Encountered");
	}
	return err;
}