from sqlalchemy import create_engine
import pandas as pd

engine = create_engine('postgresql://test_user:85dLRZHnkZHZk6Yoy0Ww0NW3ZgnW0u2J9SSkjVCbQ0@52.17.135.55:36603/test_db')

#bit_cond = "get_bit(type::bit(32)<<1, position('1' in type::bit(32))-1)=1"
bit_cond = "get_bit(type::bit(2), 0)=1"

sql = """select login,count(case when extract(epoch from age(close_time, open_time))>59 then null
                                 when exists (select 1
                                              from mt4.marked_trades
                                              where marked_trades.ticket=trades.ticket
                                                    and {0}
                                             ) then null
                                 else 1
                            end) as cnt
        from mt4.trades
        group by login""".format(bit_cond)

mt4_metric1 = pd.read_sql(sql, engine)

sql = """select trades.login, count(pair_trades.ticket) as cnt
        from mt4.trades
        left join mt4.trades pair_trades on pair_trades.login = trades.login
                                            and pair_trades.cmd <> trades.cmd
                                            and abs(extract(epoch from age(pair_trades.open_time, trades.open_time)))<=30
                                            and pair_trades.ticket > trades.ticket
        where not exists (select 1
                          from mt4.marked_trades
                          where marked_trades.ticket in (trades.ticket,pair_trades.ticket)
                                and {0}
                         )								
        group by trades.login""".format(bit_cond)

mt4_metric2 = pd.read_sql(sql, engine)

mt4_metrics = mt4_metric1.merge(mt4_metric2, on="login", how="left", suffixes=('_1', '_2'))

sql = "select distinct login from mt5.deals"

mt5_logins_df = pd.read_sql(sql, engine)

sql = """select positionid, login
        from mt5.deals
        where not exists (select 1
                          from mt5.marked_trades
                          where marked_trades.positionid = deals.positionid
                                and {0}
                         )
        group by positionid, login
        having extract(epoch from age(max(time)
                                      , min(case when entry = 0 then time end)
                                     )
                      )<60""".format(bit_cond)

mt5_deals_df = pd.read_sql(sql, engine)

mt5_metric1 = mt5_deals_df.groupby(['login']).agg(cnt=('positionid','count'))

sql = """select deals.login, count(pair_deals.positionid) as cnt
        from mt5.deals 
        left join mt5.deals pair_deals on pair_deals.login = deals.login
                                          and pair_deals.action <> deals.action
                                          and deals.entry = 0
                                          and pair_deals.entry = 0
                                          and abs(extract(epoch from age(pair_deals.time, deals.time)))<=30
                                          and pair_deals.positionid > deals.positionid
        where not exists (select 1
                          from mt5.marked_trades
                          where marked_trades.positionid in (deals.positionid, pair_deals.positionid)
                                and {0}
                         )							  
        group by deals.login""".format(bit_cond)

mt5_metric2 = pd.read_sql(sql, engine)

mt5_metrics = mt5_logins_df.merge(mt5_metric1, on="login", how="left")\
                           .merge(mt5_metric2, on="login", how="left", suffixes=('_1', '_2'))

union_metrics = pd.concat([mt4_metrics,mt5_metrics], sort=False, axis=0)\
                  .fillna(0)\
                  .astype({'cnt_1': int, 'cnt_2': int})

sql = """with trade_intervals as (
            select login, ticket, symbol, cmd, 
                   open_time - (cast(extract (second from open_time) as int) %% 30) * interval'1 second' as time_interval
            from mt4.trades 
            where not exists (select 1
                              from mt4.marked_trades
                              where marked_trades.ticket=trades.ticket
                                    and {0}
                             )	
            union all
            select login, positionid as ticket, symbol, action as cmd,
                   time - (cast(extract (second from time) as int) %% 30) * interval'1 second' as time_interval
            from mt5.deals 
            where not exists (select 1
                              from mt5.marked_trades
                              where marked_trades.positionid = deals.positionid
                                    and {0}
                             )
                  and entry=0
        )
        select trade_intervals.login as login1, trade_interval_pairs.login as login2
        from trade_intervals
        join trade_intervals as trade_interval_pairs on trade_interval_pairs.symbol = trade_intervals.symbol
                                                        and trade_interval_pairs.time_interval = trade_intervals.time_interval
                                                        and trade_interval_pairs.login <> trade_intervals.login
                                                        and trade_interval_pairs.cmd <> trade_intervals.cmd
                                                        and trade_interval_pairs.ticket >= trade_intervals.ticket
        group by trade_intervals.login, trade_interval_pairs.login
        having count(trade_interval_pairs.ticket) > 10""".format(bit_cond)

trade_period_pairs = pd.read_sql(sql, engine)

union_metrics.to_csv("trade_metrics.csv", index=False, sep=";")
trade_period_pairs.to_csv("trade_period_pairs.csv", index=False, sep=";")

