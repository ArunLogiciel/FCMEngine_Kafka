#pragma once
#include "../FeeModule/MethodInvoker.h"
#include "../FeeModule/DbDataAdapter.h"
#include "../FeeModule/TabularMethod.h"
#include "../FeeModule/TieredMethod.h"
#include "../FeeModule/RawMethod.h"
#include "../FeeModule/CodedMethod.h"
#include "../FeeModule/logger/logger.hpp"
#include "../FeeModule/ParsingException.h"
#include <algorithm>

using namespace FeeFormula;

void LSL::FeeModule::MethodInvoker::init()
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'Initializing'";
	DEBUG_LOG("FeeModule", fmt::format("{} - Initializing", __PRETTY_FUNCTION__));

	bool error = LoadAllFromTable();
	if (error)
	{
		throw std::runtime_error("Unable to Retrieve & Parse Formulas");
	}

	//LogInfo() << __PRETTY_FUNCTION__ << " 'Initialized'";
	INFO_LOG("FeeModule", fmt::format("{} - Initialized", __PRETTY_FUNCTION__));
}

void LSL::FeeModule::MethodInvoker::init(const Adjustment& adjustment)
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'Initializing'";
	DEBUG_LOG("FeeModule", fmt::format("{} - Initializing", __PRETTY_FUNCTION__));
	for (const auto& id : adjustment.planIds)
	{
		try
		{
			LoadFromTable(id);
		}
		catch (ParsingException ex)
		{
			//LogWarning() << __PRETTY_FUNCTION__ << " 'Failed To Load Functions with ID: " << id.at(0) << " Due To Some Error'";
			WARNING_LOG("FeeModule", fmt::format("{} - Failed To Load Functions with ID: {} Due To Some Error", __PRETTY_FUNCTION__, id.at(0)));
		}
	}
	//LogInfo() << __PRETTY_FUNCTION__ << " 'Initialized'";
	INFO_LOG("FeeModule", fmt::format("{} - Initialized", __PRETTY_FUNCTION__));
}

std::vector<std::string> LSL::FeeModule::MethodInvoker::ListAllFunctions()
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'called'";
	DEBUG_LOG("FeeModule", fmt::format("{} - called", __PRETTY_FUNCTION__));
	std::vector<std::string> method_list;
	for (auto& method : m_methods)
	{
		method_list.push_back(method.first + " - " + method.second->m_methodName);
	}
	return method_list;
}

bool LSL::FeeModule::MethodInvoker::LoadAllFromTable()
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'called'";
	DEBUG_LOG("FeeModule", fmt::format("{} - called", __PRETTY_FUNCTION__));
	bool ret = 1; //set to error

	std::vector<std::vector<std::string>> func_ids;
	int error = DbDataAdapter::getInstance()->dataRetrievedFormulaId(func_ids);
	if (error == 0 && !func_ids.empty())
	{
		m_methods.clear();
		ret = 0; //SUCCESS
		for (auto& id : func_ids)
		{
			try
			{
				LoadFromTable(id.at(0));
			}
			catch (ParsingException ex)
			{
				//LogWarning() << __PRETTY_FUNCTION__ << " 'Failed To Load Functions with ID: " << id.at(0) << " Due To Some Error'";
				WARNING_LOG("FeeModule", fmt::format("{} - Failed To Load Functions with ID: {} Due To Some Error", __PRETTY_FUNCTION__, id.at(0)));
			}
		}
	}
	return ret;
}

bool LSL::FeeModule::MethodInvoker::LoadFromTable(const std::string& id)
{
	using method_ptr = std::unique_ptr<Interfaces::MethodInterface>;
	//LogDebug() << __PRETTY_FUNCTION__ << " 'called for PlanId:" << id << "'";
	DEBUG_LOG("FeeModule", fmt::format("{} - called for PlanId:{}", __PRETTY_FUNCTION__, id));
	FormulaData formulaData;
	int status = DbDataAdapter::getInstance()->dataRetrievedFormula(formulaData, id);
	if (status == 0)
	{
		if (formulaData.planId != "")
		{ 
			if (formulaData.formulaType == EN_FormulaType_Tabular)
			{
				//LogDebug() << "FormulaType: Tabular";
				DEBUG_LOG("FeeModule", fmt::format("{} - FormulaType: Tabular", __PRETTY_FUNCTION__));
				auto it = m_methods.find(id);
				if (it == m_methods.end())
				{
					method_ptr temp = std::make_unique<TabularMethod>(formulaData.formulaJSON);
					temp->m_methodName = formulaData.formulaName;
					m_methods.emplace(id, std::move(temp));
				}
				else
				{
					method_ptr temp = std::make_unique<TabularMethod>(formulaData.formulaJSON);
					temp->m_methodName = formulaData.formulaName;
					it->second = std::move(temp);
				}
				return true;
			}

			else if (formulaData.formulaType == EN_FormulaType_Tiered)
			{
				//LogDebug() << "FormulaType: Tiered";
				DEBUG_LOG("FeeModule", fmt::format("{} - FormulaType: Tiered", __PRETTY_FUNCTION__));
				auto it = m_methods.find(id);
				if (it == m_methods.end())
				{
					method_ptr temp = std::make_unique<TieredMethod>(formulaData.formulaJSON);
					temp->m_methodName = formulaData.formulaName;
					m_methods.emplace(id, std::move(temp));
				}
				else
				{
					method_ptr temp = std::make_unique<TieredMethod>(formulaData.formulaJSON);
					temp->m_methodName = formulaData.formulaName;
					it->second = std::move(temp);
				}
				return true;
			}

			else if (formulaData.formulaType == EN_FormulaType_Raw)
			{
				//LogDebug() << "FormulaType: Raw";
				DEBUG_LOG("FeeModule", fmt::format("{} - FormulaType: Raw", __PRETTY_FUNCTION__));
				auto it = m_methods.find(id);
				if (it == m_methods.end())
				{
					method_ptr temp = std::make_unique<RawMethod>(formulaData.formulaJSON);
					temp->m_methodName = formulaData.formulaName;
					m_methods.emplace(id, std::move(temp));
				}
				else
				{
					method_ptr temp = std::make_unique<RawMethod>(formulaData.formulaJSON);
					temp->m_methodName = formulaData.formulaName;
					it->second = std::move(temp);
				}
				return true;
			}

			else if (formulaData.formulaType == EN_FormulaType_Coded)
			{
				//LogDebug() << "FormulaType: Coded";
				DEBUG_LOG("FeeModule", fmt::format("{} - FormulaType: Coded", __PRETTY_FUNCTION__));
				auto it = m_methods.find(id);
				if (it == m_methods.end())
				{
					method_ptr temp = std::make_unique<CodedMethod>(formulaData.formulaJSON);
					temp->m_methodName = formulaData.formulaName;
					m_methods.emplace(id, std::move(temp));
				}
				else
				{
					method_ptr temp = std::make_unique<CodedMethod>(formulaData.formulaJSON);
					temp->m_methodName = formulaData.formulaName;
					it->second = std::move(temp);
				}
				return true;
			}
			else
			{
				throw ParsingException("Invalid Formula Type");
			}
		}
	}
	return false;
}

Fee LSL::FeeModule::MethodInvoker::Invoke(std::string formulaId, OrderExecutionData& data)
{
	//LogDebug() << __PRETTY_FUNCTION__ << " 'called'";
	DEBUG_LOG("FeeModule", fmt::format("{} - called", __PRETTY_FUNCTION__));
	auto method = m_methods.find(formulaId);
	if (method != m_methods.end())
	{
		auto fee = method->second->evaluate(data);
		//LogDebug() << __PRETTY_FUNCTION__ << " '" << fee.second << " Executed from PlanID: " << method->first << "'";
		DEBUG_LOG("FeeModule", fmt::format("{} - {} Executed from PlanID: {}", __PRETTY_FUNCTION__, fee.second, method->first));
		return fee.second;
	}

	//LogDebug() << __PRETTY_FUNCTION__ << " 'Fee Return is 0.00 for " << data.orderId << "'";
	DEBUG_LOG("FeeModule", fmt::format("{} - Fee Return is 0.00 for: {}", __PRETTY_FUNCTION__, data.orderId));
	return Fee(0.00);
}

void LSL::FeeModule::MethodInvoker::finish() {
	//LogInfo() << __PRETTY_FUNCTION__ << " 'Finished Successfully'";
	INFO_LOG("FeeModule", fmt::format("{} - Finished Successfully", __PRETTY_FUNCTION__));
}
