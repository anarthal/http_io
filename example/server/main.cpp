//
// Copyright (c) 2022 Vinnie Falco (vinnie dot falco at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/beast2
//

#include "certificate.hpp"
#include "serve_detached.hpp"
#include "serve_log_admin.hpp"
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/beast2/asio_io_context.hpp>
#include <boost/beast2/server/http_server.hpp>
#include <boost/beast2/server/router.hpp>
#include <boost/beast2/server/router_types.hpp>
#include <boost/beast2/server/serve_static.hpp>
#include <boost/beast2/error.hpp>
#include <boost/capy/application.hpp>
#include <boost/http_proto/method.hpp>
#include <boost/http_proto/request_parser.hpp>
#include <boost/http_proto/serializer.hpp>
#include <boost/capy/brotli/decode.hpp>
#include <boost/capy/brotli/encode.hpp>
#include <boost/capy/zlib/deflate.hpp>
#include <boost/capy/zlib/inflate.hpp>
#include <boost/redis/connection.hpp>
#include <boost/system/error_code.hpp>
#include <iostream>

namespace boost {
namespace beast2 {

void install_services(capy::application& app)
{
#ifdef BOOST_CAPY_HAS_BROTLI
    capy::brotli::install_decode_service(app);
    capy::brotli::install_encode_service(app);
#endif

#ifdef BOOST_CAPY_HAS_ZLIB
    capy::zlib::install_deflate_service(app);
    capy::zlib::install_inflate_service(app);
#endif

    // VFALCO These ugly incantations are needed for http_proto and will hopefully go away soon.
    http_proto::install_parser_service(app,
        http_proto::request_parser::config());
    http_proto::install_serializer_service(app,
        http_proto::serializer::config());
}

class redis_client {
    redis::connection conn_;
public:
    redis_client(asio::any_io_executor ex, const redis::config& cfg): conn_(ex) {
        conn_.async_run(cfg, asio::detached);
    }

    void stop() {
        conn_.cancel();
    }

    redis::connection& get() { return conn_; }
};

using socket_type = asio::basic_stream_socket<asio::ip::tcp, asio::io_context::executor_type>;

system::error_code spawn_coroutine(
    Request& req,
    ResponseAsio<socket_type&>& res,
    std::function<asio::awaitable<system::error_code>()> awfn
)
{
    return res.detach([&res, awfn = std::move(awfn)](resumer resume){
        asio::co_spawn(
            res.stream.get_executor(),
            std::move(awfn),
            [&res, resume](std::exception_ptr exc, system::error_code ec) {
                if (exc) {
                    try {
                        std::rethrow_exception(exc);
                    } catch (const std::exception& err) {
                        std::cerr << "Exception: " << err.what() << std::endl;
                    }
                    res.status(http_proto::status::internal_server_error);
                    res.set_body("");
                    resume(route::close);
                } else {
                    resume(ec);
                }
            }
        );
    });
}

int server_main( int argc, char* argv[] )
{
    try
    {
        // Check command line arguments.
        if (argc != 5)
        {
            std::cerr << "Usage: " << argv[0] << " <address> <port> <doc_root> <num_workers>\n";
            return EXIT_FAILURE;
        }

        capy::application app;

        install_services(app);

        auto& srv = install_plain_http_server(
            app,
            argv[1],
            (unsigned short)std::atoi(argv[2]),
            std::atoi(argv[4]));
        
        redis::config cfg;
        auto& redis = app.insert<redis_client>(redis_client{app.get<asio_io_context>().get_executor(), cfg});

        srv.wwwroot.use("/", [&app](Request& req, ResponseAsio<socket_type&>& res) -> system::error_code {
            // Get the ID from the URL params
            const auto params = req.url.params();
            auto it = params.find("id");
            if (it == params.end()) {
                return route::next;
            }
            auto id = (*it).value;

            return spawn_coroutine(req, res, [&req, &res, &app, id = std::move(id)]() -> asio::awaitable<system::error_code> {
                auto& conn = app.get<redis_client>().get();
                auto redis_key = "ruben:" + id;

                // Get the key
                redis::request redis_req;
                redis_req.push("GET", redis_key);
                redis::response<std::optional<std::string>> redis_res;
                auto [ec, s] = co_await conn.async_exec(redis_req, redis_res, asio::as_tuple);
                std::cerr << "Error in GET: " << ec << std::endl;
                const auto& val = std::get<0>(redis_res).value();

                if (val.has_value()) {
                    res.status(http_proto::status::ok);
                    res.set_body(*val);
                    co_return route::send;
                } else {
                    co_return route::next;
                }
            });
        });

        srv.wwwroot.add(http_proto::method::get, "/ruben", [&app](Request& req, ResponseAsio<socket_type&>& res) -> system::error_code {
            const auto params = req.url.params();
            auto it = params.find("id");
            if (it == params.end()) {
                res.status(http_proto::status::not_found);
                res.set_body("");
                return route::send;
            }
            auto id = (*it).value;

            return spawn_coroutine(req, res, [&req, &res, &app, id = std::move(id)]() -> asio::awaitable<system::error_code> {
                auto& conn = app.get<redis_client>().get();
                auto redis_key = "ruben:" + id;

                // Do some really expensive calculation
                asio::steady_timer timer (res.stream.get_executor());
                timer.expires_after(std::chrono::seconds(4));
                co_await timer.async_wait();
                std::string body = std::to_string(std::chrono::system_clock::now().time_since_epoch().count());

                // Set the key in Redis, cache it for 30 seconds
                redis::request redis_req;
                redis_req.push("SET", redis_key, body, "EX", 30);
                co_await conn.async_exec(redis_req, redis::ignore);

                // Compose the response
                res.status(http_proto::status::ok);
                res.set_body(body);
                co_return route::send;
            });
        });


        srv.wwwroot.use("/", serve_static( argv[3] ));


        app.start();
        srv.attach();
    }
    catch( std::exception const& e )
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }    
    return EXIT_SUCCESS;
}

} // beast2
} // boost

int main(int argc, char* argv[])
{
    return boost::beast2::server_main( argc, argv );
}
