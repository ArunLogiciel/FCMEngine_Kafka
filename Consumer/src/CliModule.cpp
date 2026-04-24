#pragma once
#include "../../FeeModule/CliModule.h"
#include "../../FeeModule/ParsingException.h"
#include "../FeeModule/logger/logger.hpp"
#include "../../FeeModule/DbDataAdapter.h"

namespace LSL 
{
	namespace FeeModule 
	{
		CliModule::CliModule(Interfaces::MethodInvokerInterface &methodInvoker, const uint16_t &port) : m_methodInvoker(methodInvoker)
		{
			auto rootMenu = std::make_unique< Menu >("FCM");
			rootMenu->Insert(
				"list-functions",
				[this](std::ostream& out) 
				{
					for (const auto& id : m_methodInvoker.ListAllFunctions()) 
					{
						out << id << "\n";
					}
				},
				"This call return all funtion ids loaded.");

			rootMenu->Insert(
				"load",
				[this](std::ostream& out, std::string id) 
				{
					try 
					{
						auto loaded = m_methodInvoker.LoadFromTable(id);
						if (loaded) 
						{
							out << "The function has been successfully loaded" << std::endl;
						}
						else 
						{
							out << "The function was not loaded successfully" << std::endl;
						}
					}
					catch (const ParsingException& ex) 
					{
						out << ex.what() << std::endl;
					}
				},
				"Load the following function from database.");

			rootMenu->Insert(
				"load-all",
				[this](std::ostream& out) 
				{
					try 
					{
						auto loaded = m_methodInvoker.LoadAllFromTable();
						if (loaded) 
						{
							out << "All The functions have been successfully loaded" << std::endl;
						}
						else 
						{
							out << "Not all function(s) were not successfully loaded" << std::endl;
						}
					}
					catch (const ParsingException& ex) 
					{
						out << ex.what() << std::endl;
					}
					DbDataAdapter::getInstance()->disconnectStaticDb();
				},
				"Load the all the (refreshed) function from the database.");

			m_cli = std::make_unique<Cli>(std::move(rootMenu));
			// global exit action

			m_cli->ExitAction([](auto& out) { out << "Goodbye and thanks for all the fish.\n"; });
			// std exception custom handler

			m_scheduler = std::make_unique<cli::BoostAsioScheduler>();
			m_Server = std::make_unique<cli::BoostAsioCliTelnetServer>(*m_cli, *m_scheduler, port);
			m_Server->ExitAction([](auto& out) { out << "Terminating session" << std::endl; });
		}

		void CliModule::init() 
		{
			//LogDebug() << __PRETTY_FUNCTION__ << " 'Initializing'";
			DEBUG_LOG("FeeModule", fmt::format("{} - Initializing", __PRETTY_FUNCTION__));
			
			try
			{
				m_scheduler->Run();
			}

			catch (std::exception& ex)
			{
				//LogWarning() << __PRETTY_FUNCTION__ << " 'Exception: '" << ex.what();
				WARNING_LOG("FeeModule", fmt::format("{} - Exception:{}", __PRETTY_FUNCTION__, ex.what()));
			}
		}

		void CliModule::finish() 
		{
			try
			{
				m_scheduler->Stop(); // Must Be Called Otherwise m_scheduler won't stop
				//LogDebug() << __PRETTY_FUNCTION__ << " 'Finished Successfully'";
				DEBUG_LOG("FeeModule", fmt::format("{} - Finished Successfully", __PRETTY_FUNCTION__));
			}
			catch (std::exception& ex)
			{
				//LogWarning() << __PRETTY_FUNCTION__ << " 'Exception: '" << ex.what();
				WARNING_LOG("FeeModule", fmt::format("{} - Exception:{}", __PRETTY_FUNCTION__, ex.what()));
			}
		}
	}
}