import QuantLib as ql
import numpy as np

def create_usd_yield_curve():
    # Set the evaluation date
    today = ql.Date.todaysDate()
    ql.Settings.instance().evaluationDate = today

    # Market conventions
    calendar = ql.UnitedStates(ql.UnitedStates.GovernmentBond)
    business_convention = ql.ModifiedFollowing
    day_count = ql.Actual365Fixed()
    settlement_days = 2

    # Rate helpers
    rate_helpers = []

    # 1. Deposit rates (LIBOR or SOFR)
    deposit_conventions = [
        (3, ql.Months, ql.Sofr()),  # 3M SOFR
        (6, ql.Months, ql.Sofr())   # 6M SOFR
    ]

    for tenor, period, index in deposit_conventions:
        quote = 0.03  # Example rate, replace with current market rate
        deposit_helper = ql.DepositRateHelper(
            ql.QuoteHandle(ql.SimpleQuote(quote)),
            tenor * period,
            settlement_days,
            calendar,
            business_convention,
            True,
            day_count
        )
        rate_helpers.append(deposit_helper)

    # 2. Swap rates
    swap_tenors = [1, 2, 3, 5, 10, 15, 20, 30]
    swap_fixed_leg_frequency = ql.Annual
    swap_fixed_leg_convention = ql.Unadjusted
    swap_fixed_leg_day_count = ql.Thirty360(ql.Thirty360.US)
    swap_floating_leg_frequency = ql.Quarterly
    
    # Example swap rates (replace with current market rates)
    swap_rates = {
        1: 0.0250, 2: 0.0275, 3: 0.0300, 5: 0.0350, 
        10: 0.0400, 15: 0.0425, 20: 0.0450, 30: 0.0475
    }

    for tenor in swap_tenors:
        quote = swap_rates.get(tenor, 0.04)  # Default to 4% if not specified
        swap_index = ql.UsdLiborSwapFixedAnnual(tenor * ql.Years)
        
        swap_helper = ql.SwapRateHelper(
            ql.QuoteHandle(ql.SimpleQuote(quote)),
            tenor * ql.Years,
            calendar,
            swap_fixed_leg_frequency,
            swap_fixed_leg_convention,
            swap_fixed_leg_day_count,
            swap_index
        )
        rate_helpers.append(swap_helper)

    # Construct the yield curve
    yield_curve = ql.PiecewiseYieldCurve(
        settlement_days,
        calendar,
        rate_helpers,
        day_count,
        ql.Linear()
    )

    # Function to get zero rates
    def get_zero_rates(curve):
        zero_rates = []
        tenors = [0.25, 0.5, 1, 2, 3, 5, 10, 15, 20, 30]
        
        print("Tenor (Years) | Zero Rate")
        print("-" * 25)
        
        for tenor in tenors:
            date = calendar.advance(today, int(tenor), ql.Years)
            zero_rate = curve.zeroRate(date, day_count, ql.Continuous).rate()
            zero_rates.append((tenor, zero_rate))
            print(f"{tenor:13.2f} | {zero_rate*100:8.4f}%")
        
        return zero_rates

    # Return the curve and its zero rates
    return {
        'curve': yield_curve,
        'zero_rates': get_zero_rates(yield_curve)
    }

# Example usage
usd_curve_data = create_usd_yield_curve()