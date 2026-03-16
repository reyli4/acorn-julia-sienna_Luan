using PowerSystems
using PowerSimulations
using Dates
using Logging
using TimeSeries
using JuMP
using Gurobi
using StorageSystemsSimulations
using HydroPowerSimulations
using DataFrames
using CSV
using JSON

const PSI = PowerSimulations
const PSY = PowerSystems

# Fix: set a non-negative random seed to avoid Xoshiro DomainError
ENV["SIENNA_RANDOM_SEED"] = "12345"

include("parsing_utils.jl")
include("post_process.jl")

###############################################################################
# Load configuration
###############################################################################
config_path = length(ARGS) >= 1 ? ARGS[1] : joinpath(@__DIR__, "..", "config", "simulation.json")
config = JSON.parsefile(config_path)

sim_name   = config["sim_name"]
output_dir = config["output_dir"]
horizon    = config["horizon"]
interval   = config["interval"]

# sim_year must be passed as the second argument when called from the shell script
if length(ARGS) >= 2
    sim_year = parse(Int, ARGS[2])
else
    error("Usage: julia src/SystemSimulation.jl <config_path> <sim_year>")
end

###############################################################################
# Load system
###############################################################################
sys_file = "acorn_$(sim_year).json"
isfile(sys_file) || error("System file not found: $(sys_file). Run SystemParsing.jl first.")
println("Loading system from $(sys_file)...")
sys = PSY.System(sys_file)

###############################################################################
# Simulation setup
###############################################################################
year_output_dir = joinpath(output_dir, sim_name, string(sim_year))
mkpath(year_output_dir)

solver = optimizer_with_attributes(
    Gurobi.Optimizer,
    "TimeLimit"   => 10000.0,
    "OutputFlag"  => 0,
    "MIPGap"      => 1e-2,
)

PSY.transform_single_time_series!(sys, Hour(horizon), Hour(interval))

template_uc = PSI.template_unit_commitment(;
    network=NetworkModel(PSI.PTDFPowerModel, use_slacks=true, PTDF_matrix=PTDF(sys)),
    services=[],
)

set_device_model!(template_uc, ThermalStandard, ThermalBasicDispatch)
set_device_model!(template_uc, StandardLoad, StaticPowerLoad)
set_device_model!(template_uc, EnergyReservoirStorage, StorageDispatchWithReserves)
set_device_model!(template_uc, Transformer2W, StaticBranch)
set_device_model!(template_uc, Line, StaticBranch)
set_device_model!(template_uc, TwoTerminalGenericHVDCLine, HVDCTwoTerminalLossless)
set_device_model!(template_uc, RenewableDispatch, RenewableFullDispatch)
set_device_model!(template_uc, HydroDispatch, HydroDispatchRunOfRiver)
set_service_model!(template_uc,
    ServiceModel(TransmissionInterface, ConstantMaxInterfaceFlow; use_slacks=true))

models = SimulationModels(
    decision_models=[
        DecisionModel(
            template_uc,
            sys,
            name="UC",
            optimizer=solver,
            initialize_model=false,
            optimizer_solve_log_print=false,
            check_numerical_bounds=false,
            warm_start=true,
            store_variable_names=true,
        ),
    ],
)

sequence = SimulationSequence(models=models, ini_cond_chronology=InterProblemChronology())

steps = 365

sim = Simulation(
    name="$(sim_name)_$(sim_year)",
    steps=steps,
    models=models,
    sequence=sequence,
    simulation_folder=year_output_dir,
)

###############################################################################
# Build and execute
###############################################################################
println("Building simulation for year $(sim_year) ($(steps) steps, $(horizon)h horizon)...")
build!(sim, serialize=true)
println("Executing simulation...")
execute!(sim, enable_progress_bar=true)


###############################################################################
# Extract and save results
###############################################################################
results     = SimulationResults(sim; ignore_status=true)
results_uc  = get_decision_problem_results(results, "UC")
set_system!(results_uc, sys)
variables   = PSI.read_realized_variables(results_uc)

results_path = joinpath(results.path, "results")
mkpath(results_path)

export_results_csv(results_uc, variables, "UC", results_path)
println("Results saved to $(results_path)")

optimizer_stats = PSI.read_optimizer_stats(results_uc)
CSV.write(joinpath(results_path, "optimizer_stats.csv"), optimizer_stats)
total_objective = sum(optimizer_stats.objective_value)
println("Total simulation objective value: $(round(total_objective; digits=2))")

println("\nSimulation complete!")
println("  Year:         $(sim_year)")
println("  Steps:        $(steps)")
println("  Results path: $(results_path)")
for (name, df) in variables
    println("  $(name): $(nrow(df)) rows x $(ncol(df)-1) components")
end

exit(0)
