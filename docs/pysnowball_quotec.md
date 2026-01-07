pysnowball
雪球APP Python API (需要自取token)

快速指引
安装

pip install pysnowball
示例

>>> import pysnowball as ball
>>> ball.set_token("xq_a_token=662745a236*****;")
>>> ball.cash_flow('SH600000')
调用API前需要手动获取雪球网站的token,使用set_token设置token后才能访问雪球的API。s(xq_a_token & u)

APIs
实时行情
获取某支股票的行情数据

import pysnowball as ball
ball.quotec('SZ002027')
结果显示：

{
    "data": [
        {
            "symbol": "SZ002027",
            "current": 1.341,
            "percent": -0.89,
            "chg": -0.012,
            "timestamp": 1541486940000,
            "volume": 2695183,
            "amount": 3605340,
            "market_capital": 9835440347.54,
            "float_market_capital": null,
            "turnover_rate": null,
            "amplitude": 1.4,
            "open": 1.351,
            "last_close": 1.353,
            "high": 1.351,
            "low": 1.332,
            "avg_price": 1.338,
            "trade_volume": 22100,
            "side": 1,
            "is_trade": true,
            "level": 1,
            "trade_session": null,
            "trade_type": null,
            "current_year_percent": -35.84
        }
    ],
    "error_code": 0,
    "error_description": null
}