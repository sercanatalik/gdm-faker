import QuantLib as ql
import polars as pl
import numpy as np
from datetime import datetime, date
from typing import List, Dict, Any, Tuple

class AdvancedBondPortfolioAnalyzer:
    def __init__(self, settlement_date: date = None):
        """
        Initialize Bond Portfolio Analyzer
        
        Args:
            settlement_date (date, optional): Settlement date for calculations
        """
        # Set settlement date to today if not provided
        self.settlement_date = settlement_date or date.today()
        
        # Convert settlement date to QuantLib Date
        self.ql_settlement_date = ql.Date(
            self.settlement_date.day, 
            self.settlement_date.month, 
            self.settlement_date.year
        )
        
        # Set global evaluation date
        ql.Settings.instance().evaluationDate = self.ql_settlement_date

    def create_bond(
        self, 
        maturity_date: date,
        coupon_rate: float,
        par_value: float = 100.0,
        issue_date: date = None,
        payment_frequency: ql.Frequency = ql.Frequency.Semi_Annual,
        day_count: ql.DayCounter = ql.Actual360(),
        market_quote: float = None
    ) -> Tuple[ql.Bond, ql.YieldTermStructure]:
        """
        Create a QuantLib bond with advanced pricing capabilities
        
        Args:
            maturity_date (date): Bond maturity date
            coupon_rate (float): Annual coupon rate
            par_value (float, optional): Face value of the bond
            issue_date (date, optional): Bond issue date
            payment_frequency (ql.Frequency, optional): Coupon payment frequency
            day_count (ql.DayCounter, optional): Day count convention
            market_quote (float, optional): Market price of the bond
        
        Returns:
            Tuple of (QuantLib Bond, Yield Term Structure)
        """
        # Default issue date to 1 year before maturity if not provided
        issue_date = issue_date or date(
            maturity_date.year - 1, 
            maturity_date.month, 
            maturity_date.day
        )
        
        # Convert dates to QuantLib Dates
        ql_maturity_date = ql.Date(
            maturity_date.day, 
            maturity_date.month, 
            maturity_date.year
        )
        ql_issue_date = ql.Date(
            issue_date.day, 
            issue_date.month, 
            issue_date.year
        )
        
        # Create schedule for coupon payments
        calendar = ql.UnitedStates()
        schedule = ql.Schedule(
            ql_issue_date,
            ql_maturity_date,
            ql.Period(payment_frequency),
            calendar,
            ql.BusinessDayConvention.ModifiedFollowing,
            ql.BusinessDayConvention.ModifiedFollowing,
            ql.DateGeneration.Forward,
            False
        )
        
        # Create bond
        bond = ql.Bond(
            2,  # settlement days
            calendar,
            ql_issue_date,
            ql_maturity_date,
            schedule,
            [coupon_rate],
            day_count,
            par_value
        )
        
        # Create yield term structure
        # Use market quote if provided, otherwise use a default rate
        rate_quote = market_quote or 0.05
        rate_helper = ql.SimpleQuote(rate_quote)
        discount_curve = ql.FlatForward(
            self.ql_settlement_date, 
            ql.QuoteHandle(rate_helper), 
            day_count
        )
        
        return bond, discount_curve

    def calculate_bond_metrics(
        self, 
        bond: ql.Bond, 
        yield_curve: ql.YieldTermStructure, 
        par_value: float = 100.0
    ) -> Dict[str, Any]:
        """
        Calculate comprehensive bond metrics
        
        Args:
            bond (ql.Bond): QuantLib bond to analyze
            yield_curve (ql.YieldTermStructure): Yield term structure
            par_value (float, optional): Face value of the bond
        
        Returns:
            Dict containing various bond metrics
        """
        # Pricing engine with yield curve
        pricing_engine = ql.DiscountingBondEngine(ql.YieldTermStructureHandle(yield_curve))
        bond.setPricingEngine(pricing_engine)
        
        # Day count conventions
        day_counter = ql.Actual360()
        compounding = ql.Compounded
        frequency = ql.Annual
        
        # Calculate metrics
        metrics = {
            'clean_price': bond.cleanPrice(),
            'dirty_price': bond.dirtyPrice(),
            'accrued_interest': bond.accruedAmount(),
            
            # Yield measurements
            'yield_to_maturity': bond.bondYield(day_counter, compounding, frequency),
            'current_yield': (bond.bondYield(day_counter, compounding, frequency) * par_value) / bond.cleanPrice(),
            
            # Duration and convexity
            'macaulay_duration': ql.BondFunctions.duration(
                bond, yield_curve, day_counter, compounding, frequency
            ),
            'modified_duration': ql.BondFunctions.duration(
                bond, yield_curve, day_counter, compounding, frequency
            ) / (1 + bond.bondYield(day_counter, compounding, frequency) / frequency),
            
            'convexity': ql.BondFunctions.convexity(
                bond, yield_curve, day_counter
            ),
            
            # Z-spread calculation
            'z_spread': self.calculate_z_spread(bond, yield_curve)
        }
        
        return metrics

    def calculate_z_spread(
        self, 
        bond: ql.Bond, 
        yield_curve: ql.YieldTermStructure
    ) -> float:
        """
        Calculate Z-spread for a bond
        
        Args:
            bond (ql.Bond): QuantLib bond
            yield_curve (ql.YieldTermStructure): Yield term structure
        
        Returns:
            float: Z-spread value
        """
        try:
            # Create Z-spread calculator
            zspread_calculator = ql.BondFunctions.zSpread(
                bond, 
                ql.YieldTermStructureHandle(yield_curve), 
                ql.Actual360(), 
                ql.Compounded, 
                ql.Annual
            )
            return zspread_calculator
        except Exception:
            return np.nan

    def generate_bond_portfolio(
        self, 
        num_bonds: int = 5, 
        min_coupon: float = 0.03, 
        max_coupon: float = 0.07
    ) -> pl.DataFrame:
        """
        Generate a sample bond portfolio with advanced metrics
        
        Args:
            num_bonds (int): Number of bonds to generate
            min_coupon (float): Minimum coupon rate
            max_coupon (float): Maximum coupon rate
        
        Returns:
            pl.DataFrame: Portfolio of generated bonds with metrics
        """
        bonds_data = []
        portfolio_market_value = 0.0
        portfolio_duration = 0.0
        
        for i in range(num_bonds):
            # Random market value (position size)
            market_value = np.random.uniform(50000, 500000)
            
            # Random maturity between 1-10 years
            maturity_date = date.today().replace(
                year=date.today().year + np.random.randint(1, 11)
            )
            
            # Random coupon rate
            coupon_rate = np.random.uniform(min_coupon, max_coupon)
            
            # Random market quote (yield)
            market_quote = np.random.uniform(0.04, 0.08)
            
            # Create QuantLib bond
            bond, yield_curve = self.create_bond(
                maturity_date=maturity_date, 
                coupon_rate=coupon_rate,
                market_quote=market_quote
            )
            
            # Calculate metrics
            metrics = self.calculate_bond_metrics(bond, yield_curve)
            
            bond_info = {
                'bond_id': f'BOND_{i+1}',
                'coupon_rate': coupon_rate,
                'maturity_date': maturity_date,
                'market_value': market_value,
                
                # Pricing metrics
                'clean_price': metrics['clean_price'],
                'dirty_price': metrics['dirty_price'],
                
                # Yield metrics
                'yield_to_maturity': metrics['yield_to_maturity'],
                'current_yield': metrics['current_yield'],
                
                # Duration metrics
                'macaulay_duration': metrics['macaulay_duration'],
                'modified_duration': metrics['modified_duration'],
                
                # Additional metrics
                'convexity': metrics['convexity'],
                'z_spread': metrics['z_spread']
            }
            
            bonds_data.append(bond_info)
            
            # Portfolio-level calculations
            portfolio_market_value += market_value
            portfolio_duration += metrics['modified_duration'] * (market_value / portfolio_market_value)
        
        # Convert to DataFrame
        portfolio_df = pl.DataFrame(bonds_data)
        
        # Add portfolio-level weighted average duration
        portfolio_df = portfolio_df.with_columns(
            pl.lit(portfolio_duration).alias('portfolio_weighted_avg_duration')
        )
        
        return portfolio_df

def main():
    # Initialize portfolio analyzer
    analyzer = AdvancedBondPortfolioAnalyzer()
    
    # Generate bond portfolio
    bond_portfolio = analyzer.generate_bond_portfolio(num_bonds=10)
    
    # Display portfolio analysis
    print("Bond Portfolio Analysis:")
    print(bond_portfolio)
    
    # Calculate portfolio-level statistics
    print("\nPortfolio-Level Statistics:")
    print(f"Weighted Average Duration: {bond_portfolio['portfolio_weighted_avg_duration'][0]:.4f}")
    print(f"Average Yield to Maturity: {bond_portfolio['yield_to_maturity'].mean():.4f}")
    print(f"Total Market Value: ${bond_portfolio['market_value'].sum():,.2f}")

if __name__ == "__main__":
    main()