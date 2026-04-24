#include "../FeeModule/HttpClient.h"

std::string HttpClient::get(const std::string& target)
{
    return makeRequest(http::verb::get, target);
}

std::string HttpClient::put(const std::string& target, const std::string& body)
{
    return makeRequest(http::verb::put, target, body);
}

std::string HttpClient::post(const std::string& target, const std::string& body)
{
    return makeRequest(http::verb::post, target, body);
}

std::string HttpClient::delete_(const std::string& target)
{
    return makeRequest(http::verb::delete_, target);
}

std::string HttpClient::makeRequest(http::verb method, const std::string& target, const std::string& body)
{
    try 
    {
        // look up the domain name
        auto const results = resolver_.resolve(host_, port_);

        // make the connection on the ip address we get from a lookup
        net::connect(socket_, results.begin(), results.end());

        // set up an http request message
        http::request<http::string_body> req{ method, target, 11 };
        req.set(http::field::host, host_);
        req.set(http::field::user_agent, "FCM_Engine_Client/1.0");

        if (!body.empty()) 
        {
            req.set(http::field::content_type, "application/json");
            req.body() = body;
        }

        req.prepare_payload();

        // send the http request
        http::write(socket_, req);

        // this buffer is used for reading and must be persisted
        beast::flat_buffer buffer;

        // declare a container to hold the response
        http::response<http::dynamic_body> res;

        // receive the http response
        http::read(socket_, buffer, res);

        // gracefully close the socket
        beast::error_code ec;
        socket_.shutdown(tcp::socket::shutdown_both, ec);
        if (ec && ec != beast::errc::not_connected)
            throw beast::system_error{ ec };

        // return the response body
        auto resp = beast::buffers_to_string(res.body().data());
        //std::cout << "response: " <<resp << std::endl;
        return resp;
    }
    catch (const std::exception& e) 
    {
        return std::string("error: ") + e.what();
    }
}
