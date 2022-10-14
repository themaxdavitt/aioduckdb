aioduckdb\: DuckDB for AsyncIO
==============================

aioduckdb provides a friendly, async interface to DuckDB databases. It has been ported from the original aiosqlite module.

It replicates the ``duckdb`` module, but with async versions
of all the standard connection and cursor methods, plus context managers for
automatically closing connections and cursors::

    async with aioduckdb.connect(...) as db:
        await db.execute("INSERT INTO some_table ...")
        await db.commit()

        async with db.execute("SELECT * FROM some_table") as cursor:
            async for row in cursor:
                ...

It can also be used in the traditional, procedural manner::

    db = await aioduckdb.connect(...)
    cursor = await db.execute('SELECT * FROM some_table')
    row = await cursor.fetchone()
    rows = await cursor.fetchall()
    await cursor.close()
    await db.close()

Install
-------

aioduckdb is compatible with Python 3.6 and newer.
~~You can install it from PyPI:~~ Not currently on PyPI.


Details
-------

aioduckdb allows interaction with DuckDB databases on the main AsyncIO event
loop without blocking execution of other coroutines while waiting for queries
or data fetches.  It does this by using a single, shared thread per connection.
This thread executes all actions within a shared request queue to prevent
overlapping actions.

Connection objects are proxies to the real connections, contain the shared
execution thread, and provide context managers to handle automatically closing
connections.  Cursors are similarly proxies to the real cursors, and provide
async iterators to query results.


License
-------

aioduckdb is copyright Salvador Pardiñas, and licensed under the
MIT license.  I am providing code in this repository to you under an open source
license.  This is my personal repository; the license you receive to my code
is from me and not from my employer. See the `LICENSE`_ file for details.


Big thanks to `Amethyst Reese <https://noswap.com>`_ for the original `aiosqlite <https://github.com/omnilib/aiosqlite>`_ repository.
