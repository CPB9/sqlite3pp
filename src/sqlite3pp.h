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
#include <bmcl/Option.h>
#include <bmcl/Result.h>
#include <bmcl/StringView.h>
#include <bmcl/ArrayView.h>

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

using uint = unsigned int;

enum class Error : int {};
using OptError = bmcl::Option<Error>;
using InsError = bmcl::Result<int64_t, Error>;

class sql_str
{
    friend class statement;
public:
    sql_str(sql_str&& other);
    ~sql_str();
    const char* as_is() const;
    const char* non_null() const;
    std::string to_string() const;
private:
    sql_str(const char* p);
    const char* _p;
};

const char* to_string(Error);

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

enum class synchronous : uint8_t { Off = 0, Normal = 1, Full = 2, Extra = 3};
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
    using error_handler = std::function<void(Error, const char*)>;

    explicit database();
    database(const char* dbname, uint flags = OPEN_READWRITE | OPEN_CREATE, const char* vfs = nullptr);
    database(const std::string& dbname, uint flags = OPEN_READWRITE | OPEN_CREATE, const std::string& vfs = std::string());

    database(database&& db);
    database& operator=(database&& db);

    ~database();

    OptError connect(const char* dbname, uint flags = OPEN_READWRITE | OPEN_CREATE, const char* vfs = nullptr);
    OptError connect(const std::string& dbname, uint flags = OPEN_READWRITE | OPEN_CREATE, const std::string& vfs = std::string());
    OptError disconnect();
    bool is_connected() const;

    OptError attach(const char* dbname, const char* name);
    OptError attach(const std::string& dbname, const std::string& name);
    OptError detach(const char* name);
    OptError detach(const std::string& name);

    bmcl::Option<int64_t> last_insert_rowid() const;
    bmcl::Option<const char*> err_msg() const;
    bmcl::Option<int64_t> changes() const;

    OptError synchronous_mode(synchronous);
    OptError enable_foreign_keys(bool enable = true);
    OptError enable_triggers(bool enable = true);
    OptError enable_extended_result_codes(bool enable = true);
    const char* filename();

    static const char* version();
    static bool is_threadsafe();

    OptError execute(const char* sql);
    OptError execute(const std::string& sql);
    OptError executef(const char* sql, ...);

    OptError begin(bool immediate = false);
    OptError commit();
    OptError rollback();

    OptError set_busy_timeout(std::chrono::milliseconds timeout);

    void set_busy_handler(busy_handler h);
    void set_commit_handler(commit_handler h);
    void set_rollback_handler(rollback_handler h);
    void set_update_handler(update_handler h);
    void set_authorize_handler(authorize_handler h);
    static void set_error_handler(error_handler h);

private:
    sqlite3* db_;

    busy_handler bh_;
    commit_handler ch_;
    rollback_handler rh_;
    update_handler uh_;
    authorize_handler ah_;
    static error_handler eh_;
};

class database_error : public std::runtime_error
{
public:
    explicit database_error(bmcl::StringView msg); //+
    explicit database_error(database& db);
};

enum copy_semantic : uint8_t { copy, nocopy };

class statement : noncopyable
{
public:
    statement(database& db, bmcl::StringView stmt = nullptr);
    virtual ~statement();

    database& db();
    const database& db() const;
    OptError prepare(bmcl::StringView stmt, bmcl::StringView* left = nullptr);
    bmcl::Result<bool, Error> step();
    virtual OptError exec();
    OptError reset();
    OptError clear_bindings();
    OptError finish();
    bmcl::Option<const char*> err_msg() const;
    sql_str sql() const;

    bmcl::Result<uint, Error> bind_index(const char* name);

    OptError bind(uint idx, std::nullptr_t);
    OptError bind(uint idx, bool value);
    OptError bind(uint idx, int value);
    OptError bind(uint idx, double value);
    OptError bind(uint idx, int64_t value);
    OptError bind(uint idx, bmcl::StringView value, copy_semantic fcopy);
    OptError bind(uint idx, bmcl::Bytes value, copy_semantic fcopy);

    OptError bind(uint idx, bmcl::Option<bool> value);
    OptError bind(uint idx, bmcl::Option<int> value);
    OptError bind(uint idx, bmcl::Option<double> value);
    OptError bind(uint idx, bmcl::Option<int64_t> value);
    OptError bind(uint idx, bmcl::Option<bmcl::StringView> value, copy_semantic fcopy);
    OptError bind(uint idx, bmcl::Option<bmcl::Bytes> value, copy_semantic fcopy);

    template<typename T>
    inline OptError bind(const char* name, T&& t)
    {
        auto r = bind_index(name);
        if (r.isErr()) return r.unwrapErr();
        return bind(r.unwrap(), std::forward<T>(t));
    }

    template<typename T>
    inline OptError bind(const char* name, T&& t, copy_semantic fcopy)
    {
        auto r = bind_index(name);
        if (r.isErr()) return r.unwrapErr();
        return bind(r.unwrap(), std::forward<T>(t), fcopy);
    }

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
    database& db_;
    sqlite3_stmt* stmt_;
};

class batch
{
public:
    explicit batch(database& db);
    batch(database& db, bmcl::StringView stmt, copy_semantic fcopy);
    OptError prepare(bmcl::StringView stmt, copy_semantic fcopy);
    void reset();
    bmcl::Result<bool, Error> execute_next();
    OptError execute_all();
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

    class row
    {
    public:
        class getstream
        {
        public:
            getstream(row* rw, uint idx);

            template <class T>
            getstream& operator >> (T& value)
            {
                value = rw_->get<T>(idx_);
                ++idx_;
                return *this;
            }

        private:
            row* rw_;
            uint idx_;
        };

        explicit row(selecter* stmt);
        getstream getter(uint idx = 0);

        uint count() const;
        uint bytes(uint idx) const;
        uint bytes(bmcl::StringView name) const;
        data_type type(uint idx) const;
        data_type type(bmcl::StringView name) const;
        bool is_null(uint idx) const;
        bool is_null(bmcl::StringView name) const;
        template <class T> T get(uint idx) const;
        template <class T> T get(bmcl::StringView name) const
        {
            auto r = stmt_->column_index(name);
            if (r.isNone())
            {
                assert(false);
                return T();
            }
            return get<T>(r.unwrap());
        }

        template <class... Ts>
        std::tuple<Ts...> get_all(typename convert<Ts>::to_uint... idxs) const
        {
            return std::make_tuple(get<Ts>(idxs)...);
        }

    private:
        selecter* stmt_;
    };

    OptError exec() override;
    bool next();
    row get_row();
    uint column_count() const;
    bmcl::Option<uint> column_index(bmcl::StringView name) const;
    bmcl::StringView column_name(uint idx) const;
    bmcl::StringView column_decltype(uint idx) const;
};

class inserter : public statement
{
public:
    explicit inserter(database& db, bmcl::StringView stmt = nullptr);
    virtual ~inserter();
    InsError insert();
};

template<> bool selecter::row::get<bool>(uint idx) const;
template<> int selecter::row::get<int>(uint idx) const;
template<> int64_t selecter::row::get<int64_t>(uint idx) const;
template<> double selecter::row::get<double>(uint idx) const;
template<> bmcl::StringView selecter::row::get<bmcl::StringView>(uint idx) const;
template<> bmcl::Bytes selecter::row::get<bmcl::Bytes>(uint idx) const;

extern template bool selecter::row::get<bool>(uint idx) const;
extern template int selecter::row::get<int>(uint idx) const;
extern template int64_t selecter::row::get<int64_t>(uint idx) const;
extern template double selecter::row::get<double>(uint idx) const;
extern template bmcl::StringView selecter::row::get<bmcl::StringView>(uint idx) const;
extern template bmcl::Bytes selecter::row::get<bmcl::Bytes>(uint idx) const;

class transaction : noncopyable
{
public:
    explicit transaction(database& db, bool fcommit = false, bool immediate = false);
    ~transaction();

    OptError commit();
    OptError rollback();

private:
    database* db_;
    bool fcommit_;
};

} // namespace sqlite3pp
