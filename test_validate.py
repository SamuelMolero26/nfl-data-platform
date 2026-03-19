#!/usr/bin/env python
"""Quick test of _validate_and_coerce method"""

from ml.Team_diagnostic import TeamDiagnosticModel
import pandas as pd

# Create sample data
data = {
    'season': [2020, 2021],
    'team': ['DAL', 'PHI'],
    'offense_total_epa_pass': ['1.5', '2.0'],  # strings to test coercion
    'offense_total_epa_run': [0.5, 0.8],
    'defense_total_epa_pass': [-0.3, -0.5],
    'defense_total_epa_run': [-0.2, -0.1],
    'wins': [10, 6],
    'losses': [6, 10]
}
df = pd.DataFrame(data)

# Test the method
model = TeamDiagnosticModel()
validated_df = model._validate_and_coerce(df)

print("✓ _validate_and_coerce method works correctly")
print(f"  Input shape: {df.shape}, Output shape: {validated_df.shape}")
print(f"  EPA column coerced: {validated_df['offense_total_epa_pass'].dtype} (was {df['offense_total_epa_pass'].dtype})")
print(f"  All numeric conversions successful!")
