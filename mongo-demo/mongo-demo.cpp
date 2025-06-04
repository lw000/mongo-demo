// mongo-demo.cpp : 此文件包含 "main" 函数。程序执行将在此处开始并结束。
//

#include <iostream>
#include <vector>
#include <exception>

#include <mongocxx/exception/exception.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/pool.hpp>
#include <mongocxx/instance.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>

#include <mongocxx/events/command_started_event.hpp>
#include <mongocxx/events/command_succeeded_event.hpp>
#include <mongocxx/options/apm.hpp>

#define BUILDER_STREAM 0
#define INSERT_MANY 1
#define FIND_ALL 1
#define FIND_ONE 0

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

using bsoncxx::builder::stream::document;
using bsoncxx::builder::stream::finalize;

int main() {
    mongocxx::instance instance;
    mongocxx::uri uri("mongodb://localhost:27017/");

    // 配置APM选项
    mongocxx::options::apm apm_opts;
    apm_opts.on_command_started([](const mongocxx::events::command_started_event& event) {
        auto cmd = event.command();
        auto cmd_name = cmd.begin()->key();

        // 监控写入命令
        if (cmd_name == "insert" || cmd_name == "update" || cmd_name == "delete") {
            std::cout << "Write command started: " << cmd_name << "\n";
            std::cout << "  Database: " << event.database_name() << "\n";
            std::cout << "  Command: " << bsoncxx::to_json(cmd) << "\n";
            std::cout << std::endl;
        }
    });

    apm_opts.on_command_succeeded([](const mongocxx::events::command_succeeded_event& event) {
        auto cmd_name = event.command_name();
        auto duration = event.duration();

        if (cmd_name == "insert" || cmd_name == "update" || cmd_name == "delete") {
            std::cout << "Write command succeeded: " << cmd_name << " in " << duration << "ns\n";
            std::cout << "  Response: " << bsoncxx::to_json(event.reply()) << "\n";
            std::cout << std::endl;
        }
    });

    // 创建客户端选项
    mongocxx::options::client client_opts;
    client_opts.apm_opts(apm_opts);

    mongocxx::pool pool{ uri, client_opts };


#if 1
    auto client = pool.acquire();
#else
    mongocxx::client client(uri);
#endif // 1

    auto db = client["logs"];
    auto collection = db["collector-sevice"];

    try
    {
        //db.run_command(bsoncxx::from_json(R"({"ping":1})"));

        //collection.delete_many({});

        std::cout << "count_documents: " << collection.count_documents({}) << std::endl;

        std::cout << "estimated_document_count: " << collection.estimated_document_count({}) << std::endl;

#if 0
        auto stream = collection.watch();
        while (true) {
            for (const auto& event : stream) {
                std::cout << bsoncxx::to_json(event) << std::endl;
            }
        }
#endif // 0

#if 0
        //std::vector<bsoncxx::document::view> docs;
        //docs.push_back(make_document(kvp("title", "The Shawshank Redemption-1")));
        //docs.push_back(make_document(kvp("title", "The Shawshank Redemption-2")));
        //docs.push_back(make_document(kvp("title", "The Shawshank Redemption-3")));
        //docs.push_back(make_document(kvp("title", "The Shawshank Redemption-4")));
        //docs.push_back(make_document(kvp("title", "The Shawshank Redemption-5")));

        for (auto i : { 1 })
        {
            auto doc = bsoncxx::builder::stream::document{};
            doc << "title" << "The Shawshank Redemption" << bsoncxx::builder::stream::finalize;
            docs.push_back(doc); ;
        }

#endif // 0

#if INSERT_ONCE
        if (collection.count_documents({}) == 0) {

            for (auto i = 0; i < 1000; i++)
            {
                // 如果没有数据，先插入一些
                auto result = collection.insert_one(make_document(kvp("title", "The Shawshank Redemption - " + std::to_string(i))));

                // 检查结果
                if (result) {
                    std::cout << "成功插入 " << result->result().inserted_count() << " 条数据" << std::endl;
                }
                else {
                    std::cerr << "批量插入失败!" << std::endl;
                }
            }
        }
#endif // INSERT_ONCE
        
#if INSERT_MANY
        auto cursors = collection.indexes().list();
        for (auto& cursor : cursors)
        {
            //std::cout << cursor << std::endl;
        }
#if 0
        auto index_specification = make_document(kvp("age", 1));
        auto result = collection.create_index(index_specification.view());
        std::cout << "Index created: " << bsoncxx::to_json(result) << std::endl;
#endif // 0

        auto repeat = 0;
        do
        {
            //{
            //    "point_name" : "sin-tag-001_10",
            //    "identifier_alias" : "sin-tag-001_10",
            //    "point_path" : "root/workshop_A/Mock",
            //    "type_name" : "LLL",
            //    "priority_name" : "中",
            //    "alarm_desc" : "sin-tag-001_10-低三限报警",
            //    "alarm_status" : 1,
            //    "alarm_value" : "-58.707",
            //    "alarm_time" : {
            //    "$numberLong": "1748510196625"
            //    },
            //    "operate_type" : 1,
            //    "operator" : "SYSTEM",
            //    "operate_time" : {
            //    "$numberLong": "1748510196625"
            //    },
            //    "category_id" : {
            //    "$numberLong": "1"
            //    },
            //    "create_time" : {
            //    "$date": "2025-05-29T09:16:37.077Z"
            //    },
            //    "device_name" : "Mock"
            //}

#if BUILDER_STREAM
            std::vector<bsoncxx::document::view> docs;
            const auto INSERT_NUM = 1000;
            docs.reserve(INSERT_NUM);
            for (auto i = 0; i < INSERT_NUM; i++)
            {
                docs.emplace_back(document{} << "title" << "The Shawshank Redemption" << "age" << i + 10 << finalize);
            }
#else
            std::vector<bsoncxx::document::value> docs;
            const auto INSERT_NUM = 5000;
            docs.reserve(INSERT_NUM);
            for (auto i = 0; i < INSERT_NUM; i++)
            {
                docs.emplace_back(make_document(
                    kvp("title", "The Shawshank Redemption"),
                    kvp("age", i + 10)
                ));
            }
#endif // 0

            auto start = std::chrono::steady_clock::now();

            // 如果没有数据，先插入一些
            auto result = collection.insert_many(docs);

            auto end = std::chrono::steady_clock::now();
            auto mixed_time = std::chrono::duration<double>(end - start).count() * 1000;
            // 检查结果
            if (result) {
                std::cout << "批量成功插入 " << result->inserted_count() << " 条数据." << "消耗时间: " << mixed_time << std::endl;
            }
            else {
                std::cerr << "批量插入失败!" << std::endl;
            }
        } while (repeat++ < 1000);

#endif // INSERT_MANY
  
#if FIND_ONCE
        for (auto i = 0; i < 100; i++)
        {
            auto start = std::chrono::steady_clock::now();
            auto result = collection.find_one(make_document(kvp("age", i + 10)));
            auto end = std::chrono::steady_clock::now();
            auto mixed_time = std::chrono::duration<double>(end - start).count() * 1000;

            if (result) {
                std::cout << bsoncxx::to_json(*result) << "消耗时间: " << mixed_time << std::endl;
            }
            else {
                std::cout << "No result found" << std::endl;
            }
        }
#endif // FIND_ONCE

#if FIND_ALL
        for (auto i = 0; i < 100; i++)
        {
            auto start = std::chrono::steady_clock::now();
            auto results = collection.find(make_document(kvp("age", i + 10)));
            auto end = std::chrono::steady_clock::now();
            auto mixed_time = std::chrono::duration<double>(end - start).count() * 1000;
            std::cout << "消耗时间: " << mixed_time << std::endl;
            for (auto&& result : results)
            {
                std::cout << bsoncxx::to_json(result) << std::endl;
            }

            //if (result) {
            //    std::cout << bsoncxx::to_json(result) << "消耗时间: " << mixed_time << std::endl;
            //}
            //else {
            //    std::cout << "No result found" << std::endl;
            //}
        }
#endif // FIND_ALL
    }
    catch (const mongocxx::exception& e)
    {
        std::cout << "An exception occurred: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    
    return EXIT_SUCCESS;
}
