#include "../FeeModule/StaticDataManager.h"
#include "../FeeModule/ParsingException.h"
#include "../FeeModule/logger/logger.hpp"
#include "../FeeModule/Common.h"
#include "../FeeModule/DbDataAdapter.h"
#include <boost/algorithm/string.hpp>
#include "../FeeModule/rapidjson/document.h"
#include <regex>
#include <algorithm>



std::string HexToBase64Url(std::string_view input) {
	// Trim whitespace
	auto trim = [](std::string_view s) {
		auto issp = [](unsigned char c) { return std::isspace(c) != 0; };
		while (!s.empty() && issp(static_cast<unsigned char>(s.front()))) s.remove_prefix(1);
		while (!s.empty() && issp(static_cast<unsigned char>(s.back())))  s.remove_suffix(1);
		return s;
		};
	input = trim(input);

	if (input.empty()) throw std::invalid_argument("Empty input.");

	// Validate hex
	for (char c : input) {
		if (!std::isxdigit(static_cast<unsigned char>(c)))
			throw std::invalid_argument("Input contains non-hex characters.");
	}

	// Pad odd length
	std::string hex(input);
	if (hex.size() % 2 == 1) hex.insert(hex.begin(), '0');

	// Hex -> bytes
	auto hexNibble = [](char c)->int {
		if (c >= '0' && c <= '9') return c - '0';
		if (c >= 'a' && c <= 'f') return c - 'a' + 10;
		if (c >= 'A' && c <= 'F') return c - 'A' + 10;
		return -1;
		};

	std::vector<unsigned char> bytes;
	bytes.reserve(hex.size() / 2);
	for (size_t i = 0; i < hex.size(); i += 2) {
		int hi = hexNibble(hex[i]);
		int lo = hexNibble(hex[i + 1]);
		if (hi < 0 || lo < 0) throw std::invalid_argument("Invalid hex digit.");
		bytes.push_back(static_cast<unsigned char>((hi << 4) | lo));
	}

	// Base64 encode (standard)
	static const char* B64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
	std::string b64;
	b64.reserve(((bytes.size() + 2) / 3) * 4);

	size_t i = 0;
	while (i + 3 <= bytes.size()) {
		unsigned int v = (unsigned(bytes[i]) << 16) | (unsigned(bytes[i + 1]) << 8) | unsigned(bytes[i + 2]);
		b64.push_back(B64[(v >> 18) & 0x3F]);
		b64.push_back(B64[(v >> 12) & 0x3F]);
		b64.push_back(B64[(v >> 6) & 0x3F]);
		b64.push_back(B64[v & 0x3F]);
		i += 3;
	}

	size_t rem = bytes.size() - i;
	if (rem == 1) {
		unsigned int v = (unsigned(bytes[i]) << 16);
		b64.push_back(B64[(v >> 18) & 0x3F]);
		b64.push_back(B64[(v >> 12) & 0x3F]);
		b64.push_back('=');
		b64.push_back('=');
	}
	else if (rem == 2) {
		unsigned int v = (unsigned(bytes[i]) << 16) | (unsigned(bytes[i + 1]) << 8);
		b64.push_back(B64[(v >> 18) & 0x3F]);
		b64.push_back(B64[(v >> 12) & 0x3F]);
		b64.push_back(B64[(v >> 6) & 0x3F]);
		b64.push_back('=');
	}

	// Convert to URL-safe Base64 and remove '='
	for (char& c : b64) {
		if (c == '+') c = '-';
		else if (c == '/') c = '_';
	}
	while (!b64.empty() && b64.back() == '=') b64.pop_back();

	return b64;
}
void StaticDataManager::MakeDropCopyOrders(std::vector<std::vector<std::string>>& Report, std::priority_queue<OrderExecutionData>& orders, const std::string& executionDate, std::unordered_map<std::string, std::string> wingSpanFiles, const DateResolver& getEffectiveDate)
{
	try
	{
		for (const auto& order : Report)
		{
			try
			{
				OrderExecutionData Data{};
				tie(Data.tradeDate, Data.time) = common::get_date_time(order.at(12));
				auto temp = order.at(1);
				boost::algorithm::to_upper(temp);
				Data.accountValue = temp;
				Data.side = static_cast<Side>(order.at(4)[0]);	//FOR B/S/T/0
				Data.quantity = common::convert_to_int(order.at(8));
				if (order.at(3) == "") {
					Data.symbol = order.at(2);
				}
				else {
					Data.symbol = order.at(2) + wingSpanFiles[order.at(3)];
				}
				
				Data.price = common::convert_to_double(order.at(9));
				Data.execBroker = order.at(26);
				Data.liq = order.at(36);
				Data.orderId = order.at(5);
				Data.executionId = HexToBase64Url(order.at(15));
				Data.currency = order.at(31);
				Data.executionDate = executionDate;
				Data.tradeEntryType = TradeEntryType::EN_DropCopy; // this is added to act as a differentiator between EOD and DropCopy, might be used in future for delete
				populate_missing_vars(Data);
				Data.date = getEffectiveDate(Data.tradeDate, Data.time);
				try
				{		
					Data.fileName = std::filesystem::path(order.at(39)).filename().string();
				}
				catch (const std::exception& ex)
				{
					
				}

				Data.uniqueId = Data.accountValue + "|" + Data.orderId + "|" + Data.executionId + "|" +
					Data.date + "|" + Data.time;

				if (isRepeating(Data.uniqueId))
				{
					//LogWarning() << "Repeating Order : " << Data.uniqueId;
					WARNING_LOG("FCM-Engine", fmt::format("{} - Repeating Order :{} ", __PRETTY_FUNCTION__, Data.uniqueId));
					continue; // Ignoring Duplicate Orders
				}
				else
				{
					uniqueOrders.insert(Data.uniqueId);
					orders.emplace(Data);
				}
			}
			catch (const std::exception& e)
			{
				//LogWarning() << "Make Orders Failed coz of Improper Type or Value in Prop Report!";
				WARNING_LOG("FCM-Engine", fmt::format("{} - Make Orders Failed coz of Improper Type or Value in Prop Report!", __PRETTY_FUNCTION__));
				std::cout << "Make Orders Failed coz of Improper Type or Value in Prop Report!" << e.what() << std::endl;
			}
		}
	}
	catch (const std::exception& e)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << e.what();
		WARNING_LOG("FCM-Engine", fmt::format("{} - {} ", __PRETTY_FUNCTION__, e.what()));
		throw std::exception(fmt::format("{} - {} ", __PRETTY_FUNCTION__, e.what()).c_str());
	}
}
void StaticDataManager::MakeOrders(std::vector<std::vector<std::string>>& Report, std::priority_queue<OrderExecutionData>& orders, Adjustment& adjustment, const DateResolver& getEffectiveDate)
{
	try
	{
		for (const auto& order : Report)
		{
			try
			{
				if (order.at(0) == "Date/Time" || TestSymbolMap.count(order.at(4)) > 0)
				{
					continue; //Ignoring Test Symbols && Double Headers
				}
				OrderExecutionData Data{};
				tie(Data.tradeDate, Data.time) = common::get_date_time(order.at(0));
				auto temp = order.at(1);
				boost::algorithm::to_upper(temp);
				Data.accountValue = temp;
				Data.side = static_cast<Side>(order.at(2)[0]);	//FOR B/S/T/0
				Data.quantity = common::convert_to_int(order.at(3));
				Data.symbol = order.at(4);
				Data.price = common::convert_to_double(order.at(5));
				Data.route = order.at(6);
				Data.execBroker = order.at(7);
				Data.contra = order.at(8);
				Data.liq = order.at(9);
				Data.orderId = order.at(20);
				Data.executionId = order.at(21);
				Data.currency = order.at(31);
				Data.internalRoute = order.at(34);
				Data.internalLiq = order.at(35);
				Data.ParentValue = order.at(24); //ClearingId in TradeFile
				try
				{
					Data.capacity = static_cast<Capacity>(order.at(37)[0]);
				}
				catch (const std::exception& e)
				{
					Data.capacity = static_cast<Capacity>(0);
				}
				Data.executionDate = adjustment.date;

				populate_missing_vars(Data);

				// Compute effective business date (after-8PM rule + weekends + holidays)
				Data.date = getEffectiveDate(Data.tradeDate, Data.time);

				//Might Throw Exception for Old Props
				try
				{
					Data.dfidRecv = order.at(38);
					Data.mpidRecv = order.at(39);
					
					if (MpidIdMpidValueMap.at(AccountValueMpidMap.at(Data.accountValue)) == Data.mpidRecv)
					{
						/* code */
						Data.accountId = AccountsMap.at(Data.accountValue);
					}
					Data.fileName = order.back(); // this is done to get filename irrespective of index number, this solves filename issue in the Database
					if (std::find(adjustment.accessIds.begin(), adjustment.accessIds.end(), Data.fileName) == adjustment.accessIds.end())
					{
						adjustment.accessIds.push_back(Data.fileName);
					}
				}
				catch (const std::exception &ex)
				{
					Data.fileName = order.back(); // this is done to get filename irrespective of index number, this solves filename issue in the Database
					
					if (std::find(adjustment.accessIds.begin(), adjustment.accessIds.end(), Data.fileName) == adjustment.accessIds.end())
					{
						adjustment.accessIds.push_back(Data.fileName);
					}
					
				}
				try {
					if (MpidIdMpidValueMap.at(AccountValueMpidMap.at(Data.ParentValue)) == Data.mpidRecv)
					{
						Data.ParentId = AccountsMap.at(Data.ParentValue);
					}
				}
				catch (const std::exception& ex) {
					WARNING_LOG("FCM-Engine", fmt::format("{} - Parent Account Do Not Exist", __PRETTY_FUNCTION__));
				}

				try {
					Data.lastMarket = order.at(40);
				}
				catch(const std::exception& ex){
					WARNING_LOG("FCM-Engine", fmt::format("{} - Repeating Order", __PRETTY_FUNCTION__));
				}
				
				Data.uniqueId = Data.accountValue + "|" + Data.orderId + "|" + Data.executionId + "|" +
								Data.date + "|" + Data.time;
				
				if (isRepeating(Data.uniqueId))
				{
					//LogWarning() << "Repeating Order : " << Data.uniqueId;
					WARNING_LOG("FCM-Engine", fmt::format("{} - Repeating Order :{} ", __PRETTY_FUNCTION__, Data.uniqueId));
					continue; // Ignoring Duplicate Orders
				}
				else
				{
					uniqueOrders.insert(Data.uniqueId);
					orders.emplace(Data);
				}
			}
			catch (const std::exception &e)
			{
				//LogWarning() << "Make Orders Failed coz of Improper Type or Value in Prop Report!";
				WARNING_LOG("FCM-Engine", fmt::format("{} - Make Orders Failed coz of Improper Type or Value in Prop Report!", __PRETTY_FUNCTION__));
				//std::cout<<"Make Orders Failed coz of Improper Type or Value in Prop Report!" << e.what()<<std::endl;
			}
		}
	}
	catch (const std::exception &e)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << e.what();
		WARNING_LOG("FCM-Engine", fmt::format("{} - {} ", __PRETTY_FUNCTION__,e.what()));
		throw std::exception(fmt::format("{} - {} ", __PRETTY_FUNCTION__,e.what()).c_str());
	}
}
void StaticDataManager::MakeManualOrder(OrderExecutionData& orders, Adjustment& adjustment) {
	OrderExecutionData data;

	// Core IDs
	data.accountId = adjustment.accountId;
	data.accountValue = adjustment.accountValue;
	data.firmId = adjustment.firmId;
	
	data.mpidId = adjustment.mpidId;
	std::string isoDate = adjustment.date;
	std::string timePart;
	auto posT = isoDate.find('T');
	if (posT != std::string::npos) {

		std::string afterT = isoDate.substr(posT + 1); // "00:00:00+05:00"


		auto posPlus = afterT.find('+');
		if (posPlus != std::string::npos) {
			timePart = afterT.substr(0, posPlus); // "00:00:00"
		}
		else {
		
			timePart = afterT;
		}
	}

	data.time = timePart;

	// Dates
	data.date = adjustment.date;
	data.executionDate = adjustment.strDate.empty() ? adjustment.date : adjustment.strDate;
	data.currency = adjustment.Currency;

	data.orderId = adjustment.OrderId;
	data.executionId = "1";  // Hardcode for manual executions if that’s your case
	
	
	data.price =adjustment.price;
	data.quantity = adjustment.quantity;
	data.symbol = adjustment.symbol;  // JSON had Symbol, but not Adjustment — populate separately if available.

	// Flags and enums (defaults)
	data.penny = false;
	data.afterHours = false;
	data.beforeHours = false;
	data.lot = adjustment.Lot;  
	data.side = Side{};       // initialize to default
	data.type = InstrumentType{};  // default
	data.capacity = Capacity{};
	data.exchangeType = ExchangeType::EN_ExchangeType_NMS;

	// Multivalue
	if (!adjustment.accessIds.empty())
		data.accountId = adjustment.accessIds.front();

	orders = data;

}
void StaticDataManager::RetrieveAccountsData()
{
	AccountsMap.clear();
	ReverseAccountsMap.clear();
	AccountValueMpidMap.clear();
	MpidIdMpidValueMap.clear();

	std::vector<Account> accounts;
	DbDataAdapter::getInstance()->dataRetrievedAccounts(accounts);
	for (auto const& account : accounts)
	{
		AccountsMap.insert({ account.name, account.id });
		ReverseAccountsMap.insert({ account.id, account.name });
		AccountValueMpidMap.insert({ account.name, account.mpidId });
		MpidIdMpidValueMap.insert({account.mpidId,account.mpidValue});
	}
}
void StaticDataManager::RetrieveAccountsData(const std::vector<Account>& accounts)
{
	AccountsMap.clear();
	ReverseAccountsMap.clear();
	AccountValueMpidMap.clear();
	MpidIdMpidValueMap.clear();

	for (auto const& account : accounts)
	{
		AccountsMap.insert({ account.name, account.id });
		ReverseAccountsMap.insert({ account.id, account.name });
		AccountValueMpidMap.insert({ account.name, account.mpidId });
		MpidIdMpidValueMap.insert({ account.mpidId,account.mpidValue });
	}
}
void StaticDataManager::setWorkerMem(const std::string& WorkerSize)
{
	DbDataAdapter::getInstance()->setWorkerMemory(WorkerSize);

}
bool StaticDataManager::AccountExists(const std::string& accountVal)
{
	return (AccountsMap.count(accountVal) > 0 ? true : false);
}

void StaticDataManager::RetrieveExecutedOrderData(std::vector<std::vector<std::string>> &Report, const std::string& date)
{
	//Remove Duplicate OrderIds To Extract Old States From Database From Reports
	std::unordered_set<std::string> uniqueStrings;
	//std::string result = "(";

	std::string result;  // no opening parenthesis
	for (const auto& str : Report)
	{
		if (!str.at(20).empty() && uniqueStrings.insert(str.at(20)).second)
		{
			result += "('" + str.at(20) + "'),";  // each value gets its own parentheses
			}
		}

	// Remove trailing comma
	if (!result.empty())
		result.pop_back();
	ExecutedOrderDetails.clear();
	std::vector<std::vector<std::string>> db_orders;
	auto err = DbDataAdapter::getInstance()->dataRetrievedExecutedOrders(db_orders, date, result);//generic_query(db_orders, query, 1);
	if (err || db_orders.size() < 1)
	{
		return;
	}
	else
	{
		for (auto& fill : db_orders)
		{
			auto NewId = fill[0] + fill[3]; //OrderId + AccountValue to get Uniqueness
			auto itr = ExecutedOrderDetails.find(NewId);
			if (itr != ExecutedOrderDetails.end())
			{
				auto fill_itr = OrderFillCount.find(NewId);
				if (fill_itr != OrderFillCount.end())
				{
					OrderFillCount[NewId]++;
				}
				else
				{
					OrderFillCount.insert({ NewId,1 });
				}
				ExecutedOrderDetails[NewId].first += std::stod(fill[1]);	//Cummulative Quantity
				ExecutedOrderDetails[NewId].second += std::stod(fill[2]);	//Cummulative Price
			}
			else
			{
				auto fill_itr = OrderFillCount.find(NewId);
				if (fill_itr != OrderFillCount.end())
				{
					OrderFillCount[NewId]++;
				}
				else
				{
					OrderFillCount.insert({ NewId,1 });
				}
				double qty = static_cast<double>(std::stoll(fill[1]));
				double px = std::stod(fill[2]);
				ExecutedOrderDetails.insert(std::make_pair(NewId, std::make_pair(qty, px)));
			}
		}
	}
}

void StaticDataManager::populate_order_fill_data(OrderExecutionData& data)
{
	int64_t temp_fill_count = 0;
	double temp_cumulative_price = 0;

	auto NewId = data.orderId + data.accountValue; //OrderId + AccountValue to get Uniqueness

	auto executed_itr = ExecutedOrderDetails.find(NewId);
	if (executed_itr != ExecutedOrderDetails.end())
	{
		executed_itr->second.second += (data.price * data.quantity);
		temp_cumulative_price = executed_itr->second.second;
	}
	else
	{
		temp_cumulative_price = (data.price * data.quantity);
	}

	auto fillcount_itr = OrderFillCount.find(NewId);
	if (fillcount_itr == OrderFillCount.end())
	{
		OrderFillCount.insert({ NewId , 1 });
	}
	else
	{
		++(fillcount_itr->second);
	}

	if (data.executionId.empty())
	{
		data.executionId = std::to_string(OrderFillCount.at(NewId));
	}
	data.avgPx = temp_cumulative_price / data.execQty;
	data.fillCount = OrderFillCount.at(NewId);
}

void StaticDataManager::populate_missing_vars(OrderExecutionData& data)
{
	{
		//ExecQty-> Total Qty of all the executions/fills in the order
		auto NewId = data.orderId + data.accountValue; //OrderId + AccountValue to get Uniqueness


		auto itr = ExecutedOrderDetails.find(NewId);
		if (itr != ExecutedOrderDetails.end())
		{
			ExecutedOrderDetails[NewId].first += data.quantity;
		}
		else
		{
			ExecutedOrderDetails.insert(std::make_pair(NewId, std::make_pair(data.quantity, 0)));//(data.price * data.quantity))));
		}
		data.execQty = ExecutedOrderDetails[NewId].first;

		//ExecutionId + FillCount + AvgPx
		populate_order_fill_data(data);

		//Penny
		data.penny = (data.price < 1) ? true : false;


		if (data.time >= "09:30:00" && data.time <= "16:00:00")
		{
			data.beforeHours = false;
			data.afterHours = false;
		}
		else
		{
			//BeforeHours
			data.beforeHours = (data.time < "09:30:00") ? true : false;

			//AfterHours
			data.afterHours = (data.time >= "16:00:00") ? true : false;
		}

		//LOT
		if (data.quantity < 100)
			data.lot = Lot::EN_Lot_Odd;
		else if (std::fmod(data.quantity, 100) == 0)
			data.lot = Lot::EN_Lot_Even;
		else
			data.lot = Lot::EN_Lot_Mixed;

		//LastShares & LastPrice
		data.lastShares = data.quantity;
		data.lastPx = data.price;

		auto temp = IsOption(data.symbol);
		if (temp.first)
		{
			//Type(option) is followed by->UnderlyingSymbol
			data.type = EN_Instrument_Type_Option;
			data.underlyingSymbol = temp.second;
		}
		else
		{
			//Type Equity
			data.type = EN_Instrument_Type_Equity;
		}
	}
}

int StaticDataManager::ProcessSymbolData(std::vector<std::string>& symbolData)
{
	for (const auto& symbol_ : symbolData)
	{
		if (!symbol_.empty())
		{
			std::vector<std::string> splitted;
			boost::split(splitted, symbol_, boost::is_any_of("|"));
			try
			{
				if (splitted.size() < 4)
				{
					continue;
				}
				else if (splitted.at(2) == "" && splitted.at(3) == "Y") //Y @ 4th = > Test SYmbol
				{
					TestSymbolMap.insert({ splitted.at(0),"TestSymbol" });
				}
				//else if (splitted.at(2) == "N") // // N=>NYSE
				//{
				//	TapeMap.insert({ splitted.at(0),"A" });
				//}
				//else if (splitted.at(2) == "Q")	// Q=>NASDAQ
				//{
				//	TapeMap.insert({ splitted.at(0), "C" });
				//}
				//else
				//{
				//	TapeMap.insert({ splitted.at(0), "B" });	//B=>Regional & AMEX
				//}
			}
			catch (const std::exception &e)
			{
				std::cerr << e.what() << " - " << symbol_ << std::endl;
				continue;
			}
		}
	}
	symbolData.clear();
	return 0;
}

std::pair<bool, std::string> StaticDataManager::IsOption(std::string& symbol)
{
	// std::regex pattern("^(?=.{21}$)[A-Z]{1,6}\\s*[0-9]{6}[PC][0-9]{8}$");
    std::regex pattern("^[A-Z]+[0-9]*\\s{1,6}[0-9]{6}(C|P)[0-9]{8}$");

	if (std::regex_match(symbol, pattern))
	{
		auto temp = boost::erase_all_copy(symbol, " ");
		std::string underlyingSymbol = symbol.substr(0, temp.length() - 15);
		return { true,underlyingSymbol };
	}
	else
	{
		return { false,symbol };
	}
}


int StaticDataManager::CleanAccountTransactDbData(const std::string& date, const std::string& accountValues, const std::string& columnName)
{
	try
	{
			return  DbDataAdapter::getInstance()->cleanAccountTransactData(date, accountValues, columnName);
		}
	catch (const std::exception& ex)
	{
		//LogWarning() << "Unable To Delete Data For: " << accountValues << " Coz It doesn't exist";
		WARNING_LOG("FCM-Engine", fmt::format("{} - Unable To Delete Data For: {} ", __PRETTY_FUNCTION__, ex.what()));
		throw std::runtime_error(fmt::format("{} - Unable To Delete Data For: {} ", __PRETTY_FUNCTION__, ex.what()));
	}
}
int StaticDataManager::CleanAccountTransactDbDataFileName(const std::string& date, std::string& accountValues, const std::string& columnName)
{
	//only date case is removed because delete will now only be performed with date and filename
	try
	{
		return  DbDataAdapter::getInstance()->cleanAccountTransactDataFileName(date, accountValues, columnName);
	}
	catch (const std::exception& ex)
	{
		//LogWarning() << "Unable To Delete Data For: " << accountValues << " Coz It doesn't exist";
		WARNING_LOG("FCM-Engine", fmt::format("{} - Unable To Delete Data For: {} ", __PRETTY_FUNCTION__, ex.what()));
		throw std::runtime_error(fmt::format("{} - Unable To Delete Data For: {} ", __PRETTY_FUNCTION__, ex.what()));
	}
}
int StaticDataManager::CleanAccountTransactDbData(const std::string& date, const std::string& accountValues,const std::string& executionId, const std::string& orderId)
{
	try
	{
		if (accountValues.empty() && executionId.empty() && orderId.empty())
		{
			return DbDataAdapter::getInstance()->cleanAccountTransactData(date);
		}
		else if (executionId.empty() && orderId.empty() )
		{
	
			return  DbDataAdapter::getInstance()->cleanAccountTransactData(date, accountValues);
		}
		else
		{
			return  DbDataAdapter::getInstance()->cleanAccountTransactData(date, accountValues, executionId, orderId);
		}

	}
	catch (const std::exception& ex)
	{
		WARNING_LOG("FCM-Engine", fmt::format("{} - Error {} ", __PRETTY_FUNCTION__, accountValues));
		return 1;
	}
}

int StaticDataManager::CleanMpidTransactDbData(const std::string& date, const std::string& mpidId, bool deleteAll)
{
	if (deleteAll)
	{
		return  DbDataAdapter::getInstance()->cleanMpidTransactData(date, mpidId, true);
	}
	else
	{
		return  DbDataAdapter::getInstance()->cleanMpidTransactData(date, mpidId, false);
	}
}

int StaticDataManager::BulkExecuteWorkFlow()
{
	DbDataAdapter::getInstance()->BulkExecuteWorkFlow();
	return  0;
}
int StaticDataManager::BulkExecute()
{
	DbDataAdapter::getInstance()->BulkExecute();
	return  0;
}

int StaticDataManager::BulkExecuteOnlyMpid()
{
	DbDataAdapter::getInstance()->BulkExecuteOnlyMpid();
	return  0;
}


// ================================= BACK DATE =====================================
void StaticDataManager::RetrieveAccountDataFromDatabase(std::vector<std::vector<std::string>>& container, const std::string& accountIds,const std::string& date)
{
	
	auto err = DbDataAdapter::getInstance()->RetrieveAccountExecutedOrders(container, accountIds, date);
}

void StaticDataManager::RetrieveMpidDataFromDatabase(std::vector<std::vector<std::string>>& container, const std::string& mpidIds, const std::string& date)
{
	auto err = DbDataAdapter::getInstance()->RetrieveMpidExecutedOrders(container, mpidIds, date);
}


void StaticDataManager::MakeOrdersFromDb(std::vector<std::vector<std::string>>& Report, std::priority_queue<OrderExecutionData>& Orders)
{
	try
	{
		//int count = 0;
		for (const auto& order : Report)
		{
			OrderExecutionData Data{};
			Data.orderId = order.at(0);
			Data.executionId = order.at(1);
			Data.time = order.at(2);
			Data.date = order.at(3);
			Data.accountValue = order.at(4);
			Data.route = order.at(5);
			Data.penny = common::convert_to_bool(order.at(6));
			Data.liq = order.at(7);
			Data.price = common::convert_to_double(order.at(8));
			Data.quantity = common::convert_to_double(order.at(9));
			Data.execBroker = order.at(10);
			Data.contra = order.at(11);
			Data.internalRoute = order.at(12);
			Data.tape = order.at(13);
			Data.monthlyVolume = common::convert_to_double(order.at(14));
			Data.type = static_cast<InstrumentType> (common::convert_to_int(order.at(15)));
			Data.afterHours = common::convert_to_bool(order.at(16));
			Data.internalLiq = order.at(17);
			Data.side = static_cast<Side> (common::convert_to_int(order.at(18)));
			Data.symbol = order.at(19);
			Data.execQty = common::convert_to_double(order.at(20));
			Data.avgPx = common::convert_to_double(order.at(21));
			Data.lastShares = common::convert_to_double(order.at(22));
			Data.lastPx = common::convert_to_double(order.at(23));
			Data.capacity = static_cast<Capacity>(common::convert_to_int(order.at(24)));
			Data.fillCount = common::convert_to_int(order.at(25));
			Data.beforeHours = common::convert_to_bool(order.at(26));
			Data.lot = static_cast<Lot>(common::convert_to_int(order.at(27)));
			//Might Throw Exception for Old Props

			Data.dfidRecv = order.at(28);
			Data.mpidRecv = order.at(29);
			Data.fileName = order.at(30);
			//Data.exchangeType = static_cast<ExchangeType>(common::convert_to_int(order.at(31)));
			Data.execTransType = static_cast<ExecTransType>(common::convert_to_int(order.at(31)));
			Data.executionDate = order.at(32);
			Data.mpidMonthlyVolume = common::convert_to_double(order.at(33));
			Data.firmMonthlyVolume = common::convert_to_double(order.at(34));
			Data.lastMarket = order.at(35);
			Data.firmId = order.at(36);
			Data.tradeEntryType = static_cast<TradeEntryType>(common::convert_to_int(order.at(37)));
			Data.tradeDate = order.at(38);
			Data.ParentId = order.at(39);
			Data.ParentValue = order.at(40);
			Orders.emplace(Data);

		}
	}
	catch (const std::exception &e)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << e.what();
		WARNING_LOG("FCM-Engine", fmt::format("{} - {} ", __PRETTY_FUNCTION__, e.what()));
		throw std::exception( fmt::format("{} - {} ", __PRETTY_FUNCTION__, e.what()).c_str());
	}

	catch (...)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << " 'Error While Populating OrderExecutionData'";
		WARNING_LOG("FCM-Engine", fmt::format("{} - Error While Populating OrderExecutionData", __PRETTY_FUNCTION__));
		throw ParsingException(fmt::format("{} - Error While Populating OrderExecutionData", __PRETTY_FUNCTION__).c_str());
	}
}

void StaticDataManager::RetrieveRelevantAccountsData(Adjustment& adjustment)
{
	AccountsMap.clear();
	ReverseAccountsMap.clear();
	AccountValueMpidMap.clear();

	std::vector<Account> accounts;
	DbDataAdapter::getInstance()->RetrieveMpidAccounts(accounts, adjustment.accessId, adjustment.adjustmentType);
	//dataRetrievedAccounts(accounts);
	for (auto const& account : accounts)
	{
		AccountsMap.insert({ account.name, account.id });
		ReverseAccountsMap.insert({ account.id, account.name });
		AccountValueMpidMap.insert({ account.name, account.mpidId });
	}
}

void StaticDataManager::RetrieveMpidAccountsOnly(const std::string& mpidId)
{
	std::vector<Account> accounts;
	DbDataAdapter::getInstance()->RetrieveMpidAccounts(accounts, mpidId, EN_Adjustment_MPIDFee_Mpid);
	for (auto const& account : accounts)
	{
		mpidAccounts.emplace(account.name);
	}
}

bool StaticDataManager::accountExistsInMpid(const std::string& accountValue)
{
	return mpidAccounts.count(accountValue) > 0 ? true : false;
}

bool StaticDataManager::isRepeating(const std::string& uniqueId)
{
	return uniqueOrders.count(uniqueId) > 0 ? true : false;
}

std::string StaticDataManager::getAccountValue(const std::string& accountId)
{
	try
	{
		return ReverseAccountsMap.at(accountId);
	}
	catch (const std::exception& ex)
	{
		throw std::exception("Account Value Does Not Exist for AccountId");
	}

}
