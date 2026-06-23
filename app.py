```python
from flask import Flask, request, jsonify
from metaapi_cloud_sdk import MetaApi
import asyncio
import os
import json
import threading
from datetime import datetime, timedelta
from threading import RLock

app = Flask(__name__)

SESSION_FILE = "guardian_session.json"

SESSION_LOCK = RLock()
SHARED_SESSION = {}
META_API = None


# -------------------------------------------------
# API SINGLETON
# -------------------------------------------------
def get_api():
    global META_API

    if META_API is None:
        META_API = MetaApi(
            os.getenv("METAAPI_TOKEN")
        )

    return META_API


# -------------------------------------------------
# SAFE SESSION STORAGE
# -------------------------------------------------
def save_session(data):

    global SHARED_SESSION

    with SESSION_LOCK:

        SHARED_SESSION = data.copy()

        tmp = SESSION_FILE + ".tmp"

        with open(
            tmp,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                SHARED_SESSION,
                f,
                ensure_ascii=False
            )

        os.replace(
            tmp,
            SESSION_FILE
        )


def load_session():

    global SHARED_SESSION

    with SESSION_LOCK:

        if SHARED_SESSION:
            return SHARED_SESSION.copy()

        if not os.path.exists(
            SESSION_FILE
        ):
            return {}

        try:

            with open(
                SESSION_FILE,
                "r",
                encoding="utf-8"
            ) as f:

                SHARED_SESSION = (
                    json.load(f)
                )

            return SHARED_SESSION.copy()

        except Exception as e:

            print(
                "SESSION ERROR:",
                e
            )

            return (
                SHARED_SESSION.copy()
            )


# -------------------------------------------------
async def close_positions(
    connection
):

    try:

        positions = (
            await connection
            .get_positions()
        )

        for pos in positions:

            side = (
                "SELL"
                if pos["type"]
                ==
                "POSITION_TYPE_BUY"
                else "BUY"
            )

            try:

                await connection.create_market_order(
                    pos["symbol"],
                    side,
                    pos["volume"],
                    {
                        "positionId":
                        pos["id"]
                    }
                )

            except Exception as e:

                print(
                    "CLOSE ERROR",
                    e
                )

    except Exception as e:

        print(
            "FETCH POSITIONS",
            e
        )


# -------------------------------------------------
def start_risk_monitor():

    def worker():

        async def loop():

            while True:

                try:

                    session = (
                        load_session()
                    )

                    if not session:

                        await asyncio.sleep(
                            4
                        )

                        continue

                    account_id = (
                        session.get(
                            "account_id"
                        )
                    )

                    if not account_id:

                        await asyncio.sleep(
                            4
                        )

                        continue

                    api = get_api()

                    account = (
                        await api
                        .metatrader_account_api
                        .get_account(
                            account_id
                        )
                    )

                    if (
                        account.state
                        !=
                        "DEPLOYED"
                    ):

                        await asyncio.sleep(
                            4
                        )

                        continue

                    conn = (
                        account
                        .get_rpc_connection()
                    )

                    await conn.connect()

                    await (
                        conn
                        .wait_synchronized()
                    )

                    info = (
                        await conn
                        .get_account_information()
                    )

                    positions = (
                        await conn
                        .get_positions()
                    )

                    balance = float(
                        info.get(
                            "balance",
                            0
                        )
                    )

                    equity = float(
                        info.get(
                            "equity",
                            0
                        )
                    )

                    pnl = (
                        equity
                        -
                        balance
                    )

                    dd = max(
                        0,
                        (
                            (
                                balance
                                -
                                equity
                            )
                            /
                            balance
                            *
                            100
                        )
                        if balance
                        else 0
                    )

                    session[
                        "latest_stats"
                    ] = {

                        "session_valid": True,

                        "is_locked":
                        session.get(
                            "is_locked",
                            False
                        ),

                        "balance":
                        balance,

                        "equity":
                        equity,

                        "current_pnl":
                        pnl,

                        "daily_profit":
                        pnl,

                        "total_progress_drawdown":
                        dd,

                        "open_trades":
                        len(
                            positions
                        ),

                        "remaining_trades":
                        max(
                            0,
                            4
                            -
                            len(
                                positions
                            )
                        )
                    }

                    save_session(
                        session
                    )

                except Exception as e:

                    print(
                        "RISK LOOP",
                        str(e)
                    )

                await asyncio.sleep(
                    4
                )

        loop = (
            asyncio
            .new_event_loop()
        )

        asyncio.set_event_loop(
            loop
        )

        loop.run_until_complete(
            loop()
        )

    threading.Thread(
        target=worker,
        daemon=True
    ).start()


start_risk_monitor()


# -------------------------------------------------
@app.route(
    "/api/account-stats"
)
def stats():

    session = (
        load_session()
    )

    if (
        not session
        or
        not session.get(
            "account_id"
        )
    ):

        return jsonify({

            "session_valid":
            False,

            "reason":
            "missing_session"

        })

    return jsonify(

        session.get(
            "latest_stats",
            {

                "session_valid":
                False,

                "reason":
                "syncing"

            }

        )

    )


# -------------------------------------------------
@app.route(
"/api/disconnect",
methods=["POST"]
)
def disconnect():

    with SESSION_LOCK:

        session = (
            load_session()
        )

        session[
            "account_id"
        ] = None

        session[
            "latest_stats"
        ] = None

        save_session(
            session
        )

    return jsonify({

        "status":
        "success"

    })


# -------------------------------------------------
if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=5000,
        threaded=True
    )
```
