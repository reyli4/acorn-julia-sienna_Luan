# PTDF (Power Transfer Distribution Factor)
linearized DC power flow model

## Running the model

Path: /Users/ls2236/Projects/acorn_sienna/acorn

  # 1. Build the system (generates the json file)
  julia --project=. src/SystemParsing.jl

  # 2. Run the simulation (uses the json file created in #1)
  julia --project=. src/SystemSimulation.jl

or `./run_simulation.sh`
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


