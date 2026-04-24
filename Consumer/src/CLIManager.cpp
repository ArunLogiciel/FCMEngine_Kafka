#include "../FeeModule/CLIManager.h"

LSL::FeeModule::CLIManager::CLIManager(Interfaces::MethodInvokerInterface& methodInvoker, 
										const uint16_t port, bool isEngine): m_methodInvoker(methodInvoker)
																			,m_port(port)
																			,m_isEngine(isEngine)
																			,m_cli(m_methodInvoker, m_port)
	{}

void LSL::FeeModule::CLIManager::init() 
{
	if (!m_isEngine)
	{
		m_cli_thread = std::thread([&]()
			{
				m_cli.init();
			});
	}
}

void LSL::FeeModule::CLIManager::finish()
{
	if (!m_isEngine)
	{
		m_cli.finish();
		if (m_cli_thread.joinable())
		{
			m_cli_thread.join();
		}
	}
}