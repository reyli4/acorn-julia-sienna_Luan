# PTDF (Power Transfer Distribution Factor)
linearized DC power flow model

## Running the model

Path: /Users/ls2236/Projects/acorn_sienna/acorn

  # 1. Build the system (generates the json file)
  julia --project=. src/SystemParsing.jl

  # 2. Run the simulation (uses the json file created in #1)
  julia --project=. src/SystemSimulation.jl

## Outputs

The thermal output includes nuclear and imports

```python
{
'load': 'ActivePowerTimeSeriesParameter__StandardLoad_UC.csv', #Load
'renewable': 'ActivePowerVariable__RenewableDispatch_UC.csv', #Renewable dispatch
'thermal': 'ActivePowerVariable__ThermalStandard_UC.csv', #Thermal dispatch
'hydro': 'ActivePowerVariable__HydroDispatch_UC.csv', #Hydro dispatch
'discharge':'ActivePowerOutVariable__EnergyReservoirStorage_UC.csv', #Storage discharge
'charge': 'ActivePowerInVariable__EnergyReservoirStorage_UC.csv', #Storage charge
'unserved': 'SystemBalanceSlackUp__System_UC.csv', #Unserved load
'curtailed': 'SystemBalanceSlackDown__System_UC.csv', #Overgeneration slack
'exports': 'FlowActivePowerVariable__TwoTerminalGenericHVDCLine_UC' #Exports 
}
```

## Configuration

"sim_years": "1982:2018"
"sim_years": [1989, 1994, 1997, 2003, 2005, 2009]


src/SystemParsing.jl

```julia
acorn_input_dir = "/Users/ls2236/Projects/acorn-julia-mini/runs/p3_2012_full_gas/inputs"
climate_scenario = "historical"
sim_year = 2012
PSY.to_json(sys, "acorn_$(sim_year).json", force=true)
```
src/SystemSimulation.jl

```julia
# Load the system from JSON
sys = PSY.System("acorn_2012.json")
# Simulation setup parameters
sim_name = "acorn2040_full_gas"
output_dir = "SimResults"
horizon = 24
interval = 24
steps = 365  # Full year (365 days)
```

## Post Processing


GeneratorSummary.jl

```julia
sim_name = "acorn2040_full_gas"
```

CO2EmissionsCalc.jl

```julia
sim_name = "acorn2040_full_gas"
```

/Users/ls2236/Projects/acorn-julia/post_processing/simulation_summary.py

```python
RESULTS = Path("/Users/ls2236/Projects/acorn_sienna/acorn/SimResults/acorn2040_full_gas/results")
INPUTS  = Path("/Users/ls2236/Projects/acorn-julia-mini/runs/p3_2012_full_gas/inputs")
CONFIG  = Path("/Users/ls2236/Projects/acorn_sienna/acorn/config")
OUT     = Path("post_processing/figures")
```

