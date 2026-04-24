#include "../FeeModule/pch.h"
#include "../FeeModule/FeeCommissionStore.h"
#include "../FeeModule/DbDataAdapter.h"
#include "../FeeModule/logger/logger.hpp"
#include "boost/date_time/gregorian/gregorian.hpp"
#include "boost/algorithm/string/erase.hpp"


LSL::FeeModule::FeeCommissionStore::FeeCommissionStore(const std::string& curr_date, bool isEngine) : m_date(curr_date), m_isEngine(isEngine)
{
}

void LSL::FeeModule::FeeCommissionStore::init(AccountsStore& accountStore)
{
	try
	{
		if (m_date.substr(8, 10) != "01")	//only retrieving data when it's not the first date of the month
		{
			std::vector<MonthlyDetails> monthlyDetailsData;
			DbDataAdapter::getInstance()->dataRetrievedMonthlyDetails(monthlyDetailsData); 

			std::vector<MPIDMonthlyDetails> mpidMonthlyDetailsData;
			DbDataAdapter::getInstance()->dataRetrievedMPIDMonthlyDetails(mpidMonthlyDetailsData);

			std::vector<FirmMonthlyDetails> firmMonthlyDetailsData;
			DbDataAdapter::getInstance()->dataRetrievedFirmMonthlyDetails(firmMonthlyDetailsData);

			for (const auto& data : monthlyDetailsData)
			{
				if (data.tradeType == EN_Instrument_Type_Equity)
				{
					if (m_accountMonthlyVolumeOptAndEqt.count(data.accountValue) <= 2)
						m_accountMonthlyVolumeOptAndEqt.insert({ data.accountValue , {0.0, data.monthlyVolume} });
					else
						LogWarning() << __PRETTY_FUNCTION__ << " 'Multiple Equity Monthly Volume Data for Account:" << data.accountValue << "'";

				}
				else if (data.tradeType == EN_Instrument_Type_Option)
				{
					if (m_accountMonthlyVolumeOptAndEqt.count(data.accountValue) <= 2)
						m_accountMonthlyVolumeOptAndEqt.insert({ data.accountValue ,{data.monthlyVolume, 0.0} });
					else
						LogWarning() << __PRETTY_FUNCTION__ << " 'Multiple Option Monthly Volume Data for Account:" << data.accountValue << "'";
				}
			}

			for (const auto& data : mpidMonthlyDetailsData)
			{
				if (data.tradeType == EN_Instrument_Type_Equity)
				{
					if (m_mpidMonthlyVolumeOptAndEqt.count(data.mpidId) <= 2)
						m_mpidMonthlyVolumeOptAndEqt.insert({ data.mpidId , {0.0, data.monthlyVolume} });
					else
						LogWarning() << __PRETTY_FUNCTION__ << " 'Multiple Equity Monthly Volume Data for MPID:" << data.mpidId << "'";

				}
				else if (data.tradeType == EN_Instrument_Type_Option)
				{
					if (m_mpidMonthlyVolumeOptAndEqt.count(data.mpidId) <= 2)
						m_mpidMonthlyVolumeOptAndEqt.insert({ data.mpidId ,{data.monthlyVolume, 0.0} });
					else
						LogWarning() << __PRETTY_FUNCTION__ << " 'Multiple Option Monthly Volume Data for MPID:" << data.mpidId << "'";
				}
			}

			for (const auto& data : firmMonthlyDetailsData)
			{
				if (data.tradeType == EN_Instrument_Type_Equity)
				{
					if (m_firmMonthlyVolumeOptAndEqt.count(data.firmId) <= 2)
						m_firmMonthlyVolumeOptAndEqt.insert({ data.firmId , {0, data.monthlyVolume} });
					else
						LogWarning() << __PRETTY_FUNCTION__ << " 'Multiple Equity Monthly Volume Data for FirmId:" << data.firmId << "'";

				}
				else if (data.tradeType == EN_Instrument_Type_Option)
				{
					if (m_firmMonthlyVolumeOptAndEqt.count(data.firmId) <= 2)
						m_firmMonthlyVolumeOptAndEqt.insert({ data.firmId ,{data.monthlyVolume, 0} });
					else
						LogWarning() << __PRETTY_FUNCTION__ << " 'Multiple Option Monthly Volume Data for FirmId:" << data.firmId << "'";
				}
			}
		}
	}
	catch (std::exception& ex)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << ex.what();
		WARNING_LOG("FeeModule", fmt::format("{} - {}", __PRETTY_FUNCTION__, ex.what()));
		throw std::exception("Invalid Date Provided");
	}


	//Crash Recovery (As AK Bhai Said RESILIENCE)
	if (!m_isEngine)
	{
		std::vector<OrderExecutionData> orderExecutionData;

		//Assuming Execution Data will be part of Purge Mechanism and will contain only Valid Orders
		DbDataAdapter::getInstance()->dataRetrievedOrderExecution(orderExecutionData);
		for (auto& data : orderExecutionData)
		{
			updateFillCount(data);
			std::string date = boost::algorithm::erase_all_copy(data.date, "-");
			//Recovery of MonthlyVol for same date
			if (date == m_date)
			{
				data.accountId = accountStore.getAccountId(data.accountValue);
				auto acc = accountStore.getAccount(data.accountId);

				if (!data.accountId.empty())
				{
					data.mpidId = acc.mpidId;
				}
				updateMonthlyVolume(data);
				updateMPIdMonthlyVolume(data);
				updateFirmMonthlyVolume(data);
			}

			// update orderHistory as well (for bust/correct)
			orderHistory.insert({data.orderId + "_" + data.executionId + "_" + data.accountId, data});
		}
	}
	//LogInfo() << __PRETTY_FUNCTION__ << " 'Initialized'";
	INFO_LOG("FeeModule", fmt::format("{} - Initialized", __PRETTY_FUNCTION__));
}

void LSL::FeeModule::FeeCommissionStore::init(const Adjustment& adjustment)
{
	try
	{
		if (m_date.substr(8, 10) != "01")	//Retrieving Today's Data
		{
			std::vector<MonthlyDetails> monthlyDetailsData;
			DbDataAdapter::getInstance()->dataRetrievedMonthlyDetails(monthlyDetailsData, adjustment.date);
			std::vector<MPIDMonthlyDetails> mpidMonthlyDetailsData;
			DbDataAdapter::getInstance()->dataRetrievedMPIDMonthlyDetails(mpidMonthlyDetailsData, adjustment.date);

			std::vector<FirmMonthlyDetails> firmMonthlyDetailsData;
			DbDataAdapter::getInstance()->dataRetrievedFirmMonthlyDetails(firmMonthlyDetailsData, adjustment.date);
			for (const auto& data : monthlyDetailsData)
			{
				if (data.tradeType == EN_Instrument_Type_Equity)
				{
					if (m_accountMonthlyVolumeOptAndEqt.count(data.accountValue) <= 2)
						m_accountMonthlyVolumeOptAndEqt.insert({ data.accountValue , {0.0, data.monthlyVolume} });
					else
					{
						//LogWarning() << __PRETTY_FUNCTION__ << " 'Multiple Equity Monthly Volume Data for Account:" << data.accountValue << "'";
						WARNING_LOG("FeeModule", fmt::format("{} - Multiple Equity Monthly Volume Data for Account:{}", __PRETTY_FUNCTION__, data.accountValue));
					}
				}
				else if (data.tradeType == EN_Instrument_Type_Option)
				{
					if (m_accountMonthlyVolumeOptAndEqt.count(data.accountValue) <= 2)
						m_accountMonthlyVolumeOptAndEqt.insert({ data.accountValue ,{data.monthlyVolume, 0.0} });
					else
					{
						//LogWarning() << __PRETTY_FUNCTION__ << " 'Multiple Option Monthly Volume Data for Account:" << data.accountValue << "'";
						WARNING_LOG("FeeModule", fmt::format("{} - Multiple Option Monthly Volume Data for Account:{}", __PRETTY_FUNCTION__, data.accountValue));
					}
				}
			}
			for (const auto& data : mpidMonthlyDetailsData)
			{
				if (data.tradeType == EN_Instrument_Type_Equity)
				{
					if (m_mpidMonthlyVolumeOptAndEqt.count(data.mpidId) <= 2)
						m_mpidMonthlyVolumeOptAndEqt.insert({ data.mpidId , {0.0, data.monthlyVolume} });
					else
						LogWarning() << __PRETTY_FUNCTION__ << " 'Multiple Equity Monthly Volume Data for MPID:" << data.mpidId << "'";

				}
				else if (data.tradeType == EN_Instrument_Type_Option)
				{
					if (m_mpidMonthlyVolumeOptAndEqt.count(data.mpidId) <= 2)
						m_mpidMonthlyVolumeOptAndEqt.insert({ data.mpidId ,{data.monthlyVolume, 0.0} });
					else
						LogWarning() << __PRETTY_FUNCTION__ << " 'Multiple Option Monthly Volume Data for MPID:" << data.mpidId << "'";
				}
			}

			for (const auto& data : firmMonthlyDetailsData)
			{
				if (data.tradeType == EN_Instrument_Type_Equity)
				{
					if (m_firmMonthlyVolumeOptAndEqt.count(data.firmId) <= 2)
						m_firmMonthlyVolumeOptAndEqt.insert({ data.firmId , {0.0, data.monthlyVolume} });
					else
						LogWarning() << __PRETTY_FUNCTION__ << " 'Multiple Equity Monthly Volume Data for FirmId:" << data.firmId << "'";

				}
				else if (data.tradeType == EN_Instrument_Type_Option)
				{
					if (m_firmMonthlyVolumeOptAndEqt.count(data.firmId) <= 2)
						m_firmMonthlyVolumeOptAndEqt.insert({ data.firmId ,{data.monthlyVolume, 0.0} });
					else
						LogWarning() << __PRETTY_FUNCTION__ << " 'Multiple Option Monthly Volume Data for FirmId:" << data.firmId << "'";
				}
			}
		}
	}
	catch (std::exception& ex)
	{
		//LogWarning() << __PRETTY_FUNCTION__ << ex.what();
		WARNING_LOG("FeeModule", fmt::format("{} - {}", __PRETTY_FUNCTION__, ex.what()));
		throw std::exception("Invalid Date Provided");
	}

	//LogInfo() << __PRETTY_FUNCTION__ << " 'Initialized'";
	INFO_LOG("FeeModule", fmt::format("{} - Initialized", __PRETTY_FUNCTION__));
}

void LSL::FeeModule::FeeCommissionStore::FillState(OrderExecutionData& data)
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'called for OrderId:" << data.orderId << " | ExecutionId:" << data.executionId << "'";
	DEBUG_LOG("FeeModule", fmt::format("{} - called Order ID:{} | ExecutionId: {}", __PRETTY_FUNCTION__, data.orderId, data.executionId));
	updateMonthlyVolume(data);
	updateMPIdMonthlyVolume(data);
	updateFirmMonthlyVolume(data);
	//data.monthlyVolume = getMonthlyVolume(data);
	setMonthlyVolume(data);
	setMPIdMonthlyVolume(data);
	setFirmMonthlyVolume(data);

	if (!m_isEngine)
	{
		updateFillCount(data);
		data.fillCount = getFillCount(data);
	}

	//LogDebug() << __PRETTY_FUNCTION__ << " 'Fill Count & Monthly Volume Updated & Populated'";
	DEBUG_LOG("FeeModule", fmt::format("{} - Fill Count & Monthly Volume Updated & Populated", __PRETTY_FUNCTION__));
}

void LSL::FeeModule::FeeCommissionStore::SaveFeeAndComission(const OrderExecutionData& data, const FeeComissionData& feeCommission, const FeeComissionData& mpidFees)
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'called'";
	DEBUG_LOG("FeeModule", fmt::format("{} - called", __PRETTY_FUNCTION__));
	DbDataAdapter::getInstance()->filldataFeeCommissionsStore(data, feeCommission, mpidFees);
}


void LSL::FeeModule::FeeCommissionStore::finish()
{
	//LogInfo() << __PRETTY_FUNCTION__ << " 'Finished'";
	INFO_LOG("FeeModule", fmt::format("{} - Finished", __PRETTY_FUNCTION__));
}

void LSL::FeeModule::FeeCommissionStore::updateMonthlyVolume(const OrderExecutionData& data)
{
	if (data.type == EN_Instrument_Type_Equity)	//FOR EQUITY
	{
		MAP_MONTHLY_VOLUME::iterator it = m_accountMonthlyVolumeOptAndEqt.find(data.accountValue);

		if (it != m_accountMonthlyVolumeOptAndEqt.end())
		{
			it->second.second += data.lastShares;
		}
		else
		{
			m_accountMonthlyVolumeOptAndEqt.insert({ data.accountValue ,{0, data.lastShares} });
		}
	}
	else if (data.type == EN_Instrument_Type_Option) //FOR OPTION
	{
		MAP_MONTHLY_VOLUME::iterator it = m_accountMonthlyVolumeOptAndEqt.find(data.accountValue);

		if (it != m_accountMonthlyVolumeOptAndEqt.end())
		{
			it->second.first += data.lastShares;
		}
		else
		{
			m_accountMonthlyVolumeOptAndEqt.insert({ data.accountValue ,{data.lastShares, 0.0} });
		}
	}

}
void LSL::FeeModule::FeeCommissionStore::updateMPIdMonthlyVolume(const OrderExecutionData& data)
{
	if (data.type == EN_Instrument_Type_Equity)	//FOR EQUITY
	{
		MAP_MONTHLY_VOLUME::iterator it = m_mpidMonthlyVolumeOptAndEqt.find(data.mpidId);

		if (it != m_mpidMonthlyVolumeOptAndEqt.end())
		{
			it->second.second += data.lastShares;
		}
		else
		{
			m_mpidMonthlyVolumeOptAndEqt.insert({ data.mpidId , {0.0, data.lastShares} });
		}
	}
	else if (data.type == EN_Instrument_Type_Option) //FOR OPTION
	{
		MAP_MONTHLY_VOLUME::iterator it = m_mpidMonthlyVolumeOptAndEqt.find(data.mpidId);

		if (it != m_mpidMonthlyVolumeOptAndEqt.end())
		{
			it->second.first += data.lastShares;
		}
		else
		{
			m_mpidMonthlyVolumeOptAndEqt.insert({ data.mpidId , {data.lastShares, 0.0} });
		}
	}
}
void LSL::FeeModule::FeeCommissionStore::updateFirmMonthlyVolume(const OrderExecutionData& data)
{
	if (data.type == EN_Instrument_Type_Equity)	//FOR EQUITY
	{
		MAP_MONTHLY_VOLUME::iterator it = m_firmMonthlyVolumeOptAndEqt.find(data.firmId);

		if (it != m_firmMonthlyVolumeOptAndEqt.end())
		{
			it->second.second += data.lastShares;
		}
		else
		{
			m_firmMonthlyVolumeOptAndEqt.insert({ data.firmId , {0.0, data.lastShares} });
		}
	}
	else if (data.type == EN_Instrument_Type_Option) //FOR OPTION
	{
		MAP_MONTHLY_VOLUME::iterator it = m_firmMonthlyVolumeOptAndEqt.find(data.firmId);

		if (it != m_firmMonthlyVolumeOptAndEqt.end())
		{
			it->second.first += data.lastShares;
		}
		else
		{
			m_firmMonthlyVolumeOptAndEqt.insert({ data.firmId , {data.lastShares, 0.0} });
		}
	}
}
void LSL::FeeModule::FeeCommissionStore::setMPIdMonthlyVolume(OrderExecutionData& data)
{
	LogDebug() << __PRETTY_FUNCTION__ << " 'called for MpidId:" << data.mpidId << "'";
	if (data.type == EN_Instrument_Type_Equity)
	{
		MAP_MONTHLY_VOLUME::iterator it = m_mpidMonthlyVolumeOptAndEqt.find(data.mpidId);
		if (it != m_mpidMonthlyVolumeOptAndEqt.end())
		{
			data.mpidMonthlyVolume = it->second.second;
		}
	}

	else if (data.type == EN_Instrument_Type_Option)
	{
		MAP_MONTHLY_VOLUME::iterator it = m_mpidMonthlyVolumeOptAndEqt.find(data.mpidId);
		if (it != m_mpidMonthlyVolumeOptAndEqt.end())
		{
			data.mpidMonthlyVolume = it->second.first;
		}
	}
}
void LSL::FeeModule::FeeCommissionStore::setMonthlyVolume(OrderExecutionData& data)
{
	LogDebug() << __PRETTY_FUNCTION__ << " 'called for AccountValue:" << data.accountValue << "'";
	if (data.type == EN_Instrument_Type_Equity)
	{
		MAP_MONTHLY_VOLUME::iterator it = m_accountMonthlyVolumeOptAndEqt.find(data.accountValue);
		if (it != m_accountMonthlyVolumeOptAndEqt.end())
		{
			data.monthlyVolume = it->second.second;
		}
	}

	else if (data.type == EN_Instrument_Type_Option)
	{
		MAP_MONTHLY_VOLUME::iterator it = m_accountMonthlyVolumeOptAndEqt.find(data.accountValue);
		if (it != m_accountMonthlyVolumeOptAndEqt.end())
		{
			data.monthlyVolume = it->second.first;
		}
	}
}
void LSL::FeeModule::FeeCommissionStore::setFirmMonthlyVolume(OrderExecutionData& data)
{
	LogDebug() << __PRETTY_FUNCTION__ << " 'called for FirmId:" << data.firmId << "'";
	if (data.type == EN_Instrument_Type_Equity)
	{
		MAP_MONTHLY_VOLUME::iterator it = m_firmMonthlyVolumeOptAndEqt.find(data.firmId);
		if (it != m_firmMonthlyVolumeOptAndEqt.end())
		{
			data.firmMonthlyVolume = it->second.second;
		}
	}

	else if (data.type == EN_Instrument_Type_Option)
	{
		MAP_MONTHLY_VOLUME::iterator it = m_firmMonthlyVolumeOptAndEqt.find(data.firmId);
		if (it != m_firmMonthlyVolumeOptAndEqt.end())
		{
			data.firmMonthlyVolume = it->second.first;
		}
	}
}
void LSL::FeeModule::FeeCommissionStore::updateFillCount(const OrderExecutionData& data)
{
	MAP_FILL_COUNT::iterator it = m_orderFillCount.find(data.orderId + data.accountValue); //Combining them coz of Repeating OrderId

	if (it != m_orderFillCount.end())
	{
																					//bust				//new
		it->second = (data.execTransType == ExecTransType::EN_ExecTransType_Cancel ? it->second - 1 : it->second + 1);
	}
	else
	{
		m_orderFillCount.insert({ data.orderId + data.accountValue ,1});
	}
}


int64_t LSL::FeeModule::FeeCommissionStore::getFillCount(const OrderExecutionData& data)
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'called Order ID:" << data.orderId << "'";
	DEBUG_LOG("FeeModule", fmt::format("{} - called Order ID:{}", __PRETTY_FUNCTION__, data.orderId));
	MAP_FILL_COUNT::iterator it = m_orderFillCount.find(data.orderId + data.accountValue);

	if (it != m_orderFillCount.end())
	{
		return it->second;
	}
	return 0;
}

OrderExecutionData& LSL::FeeModule::FeeCommissionStore::getOrderHistory(const std::string& key)
{
	auto it = orderHistory.find(key);
	if (it == orderHistory.end())
	{
		std::string ex_string = "orderHistory doesn't exist for key: " + key;
		throw std::exception(ex_string.c_str());
	}
	return it->second;
}

void LSL::FeeModule::FeeCommissionStore::insertOrderHistory(OrderExecutionData& data)
{
	orderHistory.insert({data.orderId + "_" + data.executionId + "_" + data.accountId, data});
}
