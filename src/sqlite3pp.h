// sqlite3pp.h
//
// The MIT License
//
// Copyright (c) 2015 Wongoo Lee (iwongu at gmail dot com)
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

#pragma once
#define SQLITE3PP_VERSION "1.0.0"
#define SQLITE3PP_VERSION_MAJOR 1
#define SQLITE3PP_VERSION_MINOR 0
#define SQLITE3PP_VERSION_PATCH 0

#include <chrono>
#include <functional>
#include <iterator>
#include <string>
#include <tuple>
#include <cassert>
#include "bmcl/Option.h"
#include "bmcl/Result.h"
#include "bmcl/StringView.h"
#include "bmcl/ArrayView.h"

struct sqlite3;
struct sqlite3_stmt;

namespace sqlite3pp
{
namespace ext
{
    class function;
    class aggregate;
}

template <class T>
struct convert
{
    using to_uint = unsigned int;
};

typedef unsigned int uint;

typedef int Error;
std::string to_string(Error);

enum FileFlags
{
    OPEN_READONLY         = 0x00000001,  /* Ok for sqlite3_open_v2() */
    OPEN_READWRITE        = 0x00000002,  /* Ok for sqlite3_open_v2() */
    OPEN_CREATE           = 0x00000004,  /* Ok for sqlite3_open_v2() */
    OPEN_DELETEONCLOSE    = 0x00000008,  /* VFS only */
    OPEN_EXCLUSIVE        = 0x00000010,  /* VFS only */
    OPEN_AUTOPROXY        = 0x00000020,  /* VFS only */
    OPEN_URI              = 0x00000040,  /* Ok for sqlite3_open_v2() */
    OPEN_MEMORY           = 0x00000080,  /* Ok for sqlite3_open_v2() */
    OPEN_MAIN_DB          = 0x00000100,  /* VFS only */
    OPEN_TEMP_DB          = 0x00000200,  /* VFS only */
    OPEN_TRANSIENT_DB     = 0x00000400,  /* VFS only */
    OPEN_MAIN_JOURNAL     = 0x00000800,  /* VFS only */
    OPEN_TEMP_JOURNAL     = 0x00001000,  /* VFS only */
    OPEN_SUBJOURNAL       = 0x00002000,  /* VFS only */
    OPEN_MASTER_JOURNAL   = 0x00004000,  /* VFS only */
    OPEN_NOMUTEX          = 0x00008000,  /* Ok for sqlite3_open_v2() */
    OPEN_FULLMUTEX        = 0x00010000,  /* Ok for sqlite3_open_v2() */
    OPEN_SHAREDCACHE      = 0x00020000,  /* Ok for sqlite3_open_v2() */
    OPEN_PRIVATECACHE     = 0x00040000,  /* Ok for sqlite3_open_v2() */
    OPEN_WAL              = 0x00080000,  /* VFS only */
};

enum class data_type : uint8_t { Integer = 1, Float = 2, Text = 3, Blob = 4, Null = 5 };


class noncopyable
{
protected:
    noncopyable() = default;
    ~noncopyable() = default;

//     noncopyable(noncopyable&&) = default;
//     noncopyable& operator=(noncopyable&&) = default;

    noncopyable(noncopyable const&) = delete;
    noncopyable& operator=(noncopyable const&) = delete;
};

class database : noncopyable
{
    friend class statement;
    friend class database_error;
    friend class ext::function;
    friend class ext::aggregate;

public:
    using busy_handler = std::function<int(int)>;
    using commit_handler = std::function<int()>;
    using rollback_handler = std::function<void()>;
    using update_handler = std::function<void (int, char const*, char const*, long long int)>;
    using authorize_handler = std::function<int (int, char const*, char const*, char const*, char const*)>;

    explicit database();
    database(const char* dbname, uint flags = OPEN_READWRITE | OPEN_CREATE, const char* vfs = nullptr);
    database(const std::string& dbname, uint flags = OPEN_READWRITE | OPEN_CREATE, const std::string& vfs = std::string());

    database(database&& db);
    database& operator=(database&& db);

    ~database();

    bmcl::Option<Error> connect(const char* dbname, uint flags = OPEN_READWRITE | OPEN_CREATE, const char* vfs = nullptr);
    bmcl::Option<Error> connect(const std::string& dbname, uint flags = OPEN_READWRITE | OPEN_CREATE, const std::string& vfs = std::string());
    bmcl::Option<Error> disconnect();

    bmcl::Option<Error> attach(const char* dbname, const char* name);
    bmcl::Option<Error> attach(const std::string& dbname, const std::string& name);
    bmcl::Option<Error> detach(const char* name);
    bmcl::Option<Error> detach(const std::string& name);

    bmcl::Option<uint64_t> last_insert_rowid() const;

    bmcl::Option<Error> enable_foreign_keys(bool enable = true);
    bmcl::Option<Error> enable_triggers(bool enable = true);
    bmcl::Option<Error> enable_extended_result_codes(bool enable = true);

    Error error_code() const;
    char const* error_msg() const;

    bmcl::Option<Error> execute(const char* sql);
    bmcl::Option<Error> execute(const std::string& sql);
    bmcl::Option<Error> executef(const char* sql, ...);

    bmcl::Option<Error> commit();
    bmcl::Option<Error> rollback();

    bmcl::Option<Error> set_busy_timeout(std::chrono::milliseconds timeout);

    void set_busy_handler(busy_handler h);
    void set_commit_handler(commit_handler h);
    void set_rollback_handler(rollback_handler h);
    void set_update_handler(update_handler h);
    void set_authorize_handler(authorize_handler h);

private:
    sqlite3* db_;

    busy_handler bh_;
    commit_handler ch_;
    rollback_handler rh_;
    update_handler uh_;
    authorize_handler ah_;
};

class database_error : public std::runtime_error
{
public:
    explicit database_error(bmcl::StringView msg); //+
    explicit database_error(database& db);
};

enum copy_semantic { copy, nocopy };

class statement : noncopyable
{
public:
    statement(database& db, bmcl::StringView stmt = nullptr);
    virtual ~statement();

    bmcl::Option<Error> prepare(bmcl::StringView stmt, bmcl::StringView* left);
    bmcl::Result<bool, Error> step();
    bmcl::Option<Error> reset();
    bmcl::Option<Error> clear_bindings();
    bmcl::Option<Error> finish();
    const char* sql() const;

    bmcl::Option<Error> bind(uint idx, int value);
    bmcl::Option<Error> bind(uint idx, double value);
    bmcl::Option<Error> bind(uint idx, int64_t value);
    bmcl::Option<Error> bind(uint idx, bmcl::StringView value, copy_semantic fcopy);
    bmcl::Option<Error> bind(uint idx, bmcl::Bytes value, copy_semantic fcopy);
    bmcl::Option<Error> bind(uint idx, nullptr_t);

    bmcl::Option<Error> bind(const char* name,          int value);
    bmcl::Option<Error> bind(const std::string& name,   int value);
    bmcl::Option<Error> bind(const char* name,          double value);
    bmcl::Option<Error> bind(const std::string& name,   double value);
    bmcl::Option<Error> bind(const char* name,          int64_t value);
    bmcl::Option<Error> bind(const std::string& name,   int64_t value);
    bmcl::Option<Error> bind(const char* name,          bmcl::StringView value, copy_semantic fcopy);
    bmcl::Option<Error> bind(const std::string& name,   bmcl::StringView value, copy_semantic fcopy);
    bmcl::Option<Error> bind(const char* name,          bmcl::Bytes value, copy_semantic fcopy);
    bmcl::Option<Error> bind(const std::string& name,   bmcl::Bytes value, copy_semantic fcopy);
    bmcl::Option<Error> bind(const char* name,          nullptr_t);
    bmcl::Option<Error> bind(const std::string& name,   nullptr_t);

    class bindstream
    {
    public:
        bindstream(statement& stmt, uint idx);

        template <class T>
        bindstream& operator << (T value)
        {
            auto rc = stmt_.bind(idx_, value);
            if (rc.isSome())
            {
                assert(false);
                throw database_error(stmt_.db_);
            }
            ++idx_;
            return *this;
        }
        bindstream& operator << (bmcl::StringView value);
    private:
        statement& stmt_;
        uint idx_;
    };
    bindstream binder(uint idx = 1);

protected:
    bmcl::Option<Error> prepare_impl(bmcl::StringView stmt, bmcl::StringView* left);
    bmcl::Option<Error> finish_impl(sqlite3_stmt* stmt);

protected:
    database& db_;
    sqlite3_stmt* stmt_;
};

class batch
{
public:
    explicit batch(database& db);
    batch(database& db, bmcl::StringView stmt, copy_semantic fcopy);
    bmcl::Option<Error> prepare(bmcl::StringView stmt, copy_semantic fcopy);
    void reset();
    bmcl::Result<bool, Error> execute_next();
    bmcl::Option<Error> execute_all();
    bmcl::StringView state() const;
private:
    database& db_;
    bmcl::Option<std::string> data_;
    bmcl::StringView state_;
    bmcl::StringView orig_;
};

class selecter : public statement
{
public:
    explicit selecter(database& db, bmcl::StringView stmt = nullptr);
    virtual ~selecter();

    class rows
    {
    public:
        class getstream
        {
        public:
            getstream(rows* rws, uint idx);

            template <class T>
            getstream& operator >> (T& value) {
                value = rws_->get<T>(idx_);
                ++idx_;
                return *this;
            }

        private:
            rows* rws_;
            uint idx_;
        };

        explicit rows(sqlite3_stmt* stmt);

        uint data_count() const;
        data_type column_type(uint idx) const;

        uint column_bytes(uint idx) const;

        template <class T> T get(uint idx) const;

        template <class... Ts>
        std::tuple<Ts...> get_columns(typename convert<Ts>::to_uint... idxs) const
        {
            return std::make_tuple(get<Ts>(idxs)...);
        }

        getstream getter(uint idx = 0);

    private:
        sqlite3_stmt* stmt_;
    };

    class query_iterator : public std::iterator<std::input_iterator_tag, rows>
    {
    public:
        query_iterator();
        explicit query_iterator(selecter* cmd);

        bool operator==(query_iterator const&) const;
        bool operator!=(query_iterator const&) const;
        query_iterator& operator++();
        value_type operator*() const;
        inline bmcl::Option<Error> error() const { return rc_; }

    private:
        selecter* cmd_;
        bool  isDone_;
        bmcl::Option<Error> rc_;
    };

    uint column_count() const;

    char const* column_name(uint idx) const;
    char const* column_decltype(uint idx) const;

    using iterator = query_iterator;

    iterator begin();
    iterator end();
};

class inserter : public statement
{
public:
    explicit inserter(database& db, bmcl::StringView stmt = nullptr);
    virtual ~inserter();
    bmcl::Result<uint64_t, Error> insert();
};

template<> int selecter::rows::get<int>(uint idx) const;
template<> int64_t selecter::rows::get<int64_t>(uint idx) const;
template<> double selecter::rows::get<double>(uint idx) const;
template<> std::string selecter::rows::get<std::string>(uint idx) const;
template<> const char* selecter::rows::get<const char*>(uint idx) const;
template<> bmcl::Bytes selecter::rows::get<bmcl::Bytes>(uint idx) const;

extern template int selecter::rows::get<int>(uint idx) const;
extern template int64_t selecter::rows::get<int64_t>(uint idx) const;
extern template double selecter::rows::get<double>(uint idx) const;
extern template std::string selecter::rows::get<std::string>(uint idx) const;
extern template const char* selecter::rows::get<const char*>(uint idx) const;
extern template bmcl::Bytes selecter::rows::get<bmcl::Bytes>(uint idx) const;

class transaction : noncopyable
{
public:
    explicit transaction(database& db, bool fcommit = false, bool freserve = false);
    ~transaction();

    bmcl::Option<Error> commit();
    bmcl::Option<Error> rollback();

private:
    database* db_;
    bool fcommit_;
};

} // namespace sqlite3pp
