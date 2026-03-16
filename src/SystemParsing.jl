using CSV
using DataFrames
using Dates
using JSON
using TimeSeries
using InfrastructureSystems
using PowerSystems
const PSY = PowerSystems
const IS = InfrastructureSystems

include("parsing_utils.jl")

###############################################################################
# Load configuration
###############################################################################
config_path = length(ARGS) >= 1 ? ARGS[1] : joinpath(@__DIR__, "..", "config", "simulation.json")
config = JSON.parsefile(config_path)

acorn_input_dir    = config["acorn_input_dir"]
climate_scenario   = config["climate_scenario"]
base_power         = Float64(config["base_power"])
fuel_price_file    = config["fuel_price_file"]
fuel_price_ts_year = config["fuel_price_ts_year"]
lmp_price_file     = config["lmp_price_file"]

# sim_year passed as second argument from run_simulation.sh
length(ARGS) >= 2 || error("Usage: julia src/SystemParsing.jl <config_path> <sim_year>")
sim_year = parse(Int, ARGS[2])
println("Processing year: $(sim_year)")

###############################################################################
# Load static data (same across all years)
###############################################################################
df_bus          = CSV.read("config/bus_config.csv", DataFrame)
df_branch       = CSV.read("config/branch_config.csv", DataFrame)
df_hvdc         = CSV.read("config/hvdc_config.csv", DataFrame)
df_iflim        = CSV.read("config/interfaceflow_limits.csv", DataFrame)
df_ifmap        = CSV.read("config/interfaceflow_mapping.csv", DataFrame)
df_sienna_thermal = CSV.read("config/thermal_config.csv", DataFrame)
df_agg          = CSV.read("config/agggen_config.csv", DataFrame)

df_acorn_ng      = CSV.read("$(acorn_input_dir)/genprop_NG_matched.csv", DataFrame, stringtype=String)
df_acorn_nuclear = CSV.read("$(acorn_input_dir)/genprop_nuclear_matched.csv", DataFrame, stringtype=String)
df_acorn_hydro   = CSV.read("$(acorn_input_dir)/genprop_hydro.csv", DataFrame, stringtype=String)
df_storage       = CSV.read("$(acorn_input_dir)/storage_assignment.csv", DataFrame)

# Fuel prices (fixed year, expanded to hourly)
fuel_cost_raw = CSV.read(fuel_price_file, DataFrame)
hourly_fuelcost = repeat(fuel_cost_raw, inner=168)
fuel_ts_start = DateTime("$(fuel_price_ts_year)-01-01T00:00:00")
fuel_ts_range = fuel_ts_start:Hour(1):(fuel_ts_start + Hour(8759))
hourly_fuelcost = hourly_fuelcost[1:length(fuel_ts_range), :]
hourly_fuelcost.TimeStamp = collect(fuel_ts_range)

# LMP prices (fixed year)
df_hourlylmp = CSV.read(lmp_price_file, DataFrame)

# Generator matching (static, same for all years)
pm_mapping = Dict(
    "Combustion Turbine" => PrimeMovers.CT,
    "Combined Cycle"     => PrimeMovers.CC,
    "Internal Combustion"=> PrimeMovers.IC,
    "Steam Turbine"      => PrimeMovers.ST,
    "Jet Engine"         => PrimeMovers.GT,
)
fuel_mapping = Dict(
    "Kerosene"    => ThermalFuels.DISTILLATE_FUEL_OIL,
    "Natural Gas" => ThermalFuels.NATURAL_GAS,
    "Fuel Oil 2"  => ThermalFuels.DISTILLATE_FUEL_OIL,
    "Coal"        => ThermalFuels.COAL,
    "Fuel Oil 6"  => ThermalFuels.RESIDUAL_FUEL_OIL,
    "NG"          => ThermalFuels.NATURAL_GAS,
)
zonename_mapping = Dict(
    "NEISO" => "NPX",
    "PJM"   => "PJM",
    "IESO"  => "O H",
    "HQ"    => "H Q",
)
matches = match_thermal_generators(df_acorn_ng, df_sienna_thermal)

###############################################################################
# Build static grid (buses, branches, generators — no time series)
###############################################################################
println("Building static grid...")
sys = PSY.System(base_power)
set_units_base_system!(sys, PSY.UnitSystem.NATURAL_UNITS)

# Zones
zone_list = unique(df_bus[!, "zone"])
for zone in zone_list
    PSY.add_component!(sys, PSY.Area(zone, 0.0, 0.0))
end

# Buses
for (_, bus) in enumerate(eachrow(df_bus))
    number = bus.busIdx
    name = bus.name * "_" * string(bus.Vn)
    area = get_component(PSY.Area, sys, bus.zone)
    _build_bus(sys, number, name, bus.busType, bus.a0, bus.v0,
               (min=bus.vmin, max=bus.vmax), bus.Vn, area)
end

# Transmission lines and transformers
br_name_list = Set()
for (_, br) in enumerate(eachrow(df_branch))
    from_bus = first(get_components(x -> PSY.get_number(x) == br.from, ACBus, sys))
    to_bus   = first(get_components(x -> PSY.get_number(x) == br.to,   ACBus, sys))
    name = string(br.from) * "-" * string(br.to)
    if name in br_name_list
        name = name * "~2"
    end
    push!(br_name_list, name)
    rating = br.rating_A != 0.0 ? br.rating_A : 99999.0
    if PSY.get_base_voltage(from_bus) == PSY.get_base_voltage(to_bus)
        _build_lines(sys; frombus=from_bus, tobus=to_bus, name=name, r=br.r, x=br.x, b=br.b, rating=rating)
    else
        _build_transformers(sys; frombus=from_bus, tobus=to_bus, name=name, r=br.r, x=br.x, b=br.b, rating=rating)
    end
end

# HVDC
for (_, hvdc) in enumerate(eachrow(df_hvdc))
    from_bus = first(get_components(x -> PSY.get_number(x) == hvdc.from_bus, ACBus, sys))
    to_bus   = first(get_components(x -> PSY.get_number(x) == hvdc.to_bus,   ACBus, sys))
    _build_hvdc(sys; frombus=from_bus, tobus=to_bus, name=hvdc.name, r=0.0, x=0.0, b=0.0, rating=hvdc.Pmax)
end

# Interface flow limits
for idx = 1:nrow(df_iflim)
    name = "IF_" * string(idx)
    rating_lb  = df_iflim[df_iflim.index.==Int(idx), :rating_lb][1]
    rating_ub  = df_iflim[df_iflim.index.==Int(idx), :rating_ub][1]
    setoflines = df_ifmap[df_ifmap.index.==Int(idx), :mapping]
    signofline = float(df_ifmap[df_ifmap.index.==Int(idx), :sign])
    ifdict = Dict(zip(string.(setoflines), signofline))
    _build_interface_flow(sys; name, rating_lb, rating_ub, ifdict)
end

# Thermal generators (no time series yet)
println("Building thermal generators...")
thermal_name_counts = Dict{String,Int}()
matched_count   = 0
unmatched_count = 0
for (i, gen) in enumerate(eachrow(df_acorn_ng))
    base_name = strip_acorn_name(gen.GEN_NAME)
    thermal_name_counts[base_name] = get(thermal_name_counts, base_name, 0) + 1
    acorn_name = thermal_name_counts[base_name] > 1 ? "$(base_name)_u$(thermal_name_counts[base_name])" : base_name

    bus  = first(get_components(x -> PSY.get_number(x) == gen.GEN_BUS, ACBus, sys))
    pmin = gen.PMIN
    pmax = gen.PMAX
    sienna_match = matches[i]

    if sienna_match !== nothing
        global matched_count += 1
        heatrate1 = sienna_match.HeatRateLM_1
        heatrate0 = sienna_match.HeatRateLM_0
        zone = sienna_match.Zone
        fuel_type_str = sienna_match.FuelType
        heat_rate, fuel_cost_ts = _add_fuel_cost(heatrate1, heatrate0, zone, fuel_type_str, pmin, hourly_fuelcost)
        pm   = haskey(pm_mapping, sienna_match.UnitType) ? pm_mapping[sienna_match.UnitType] : PrimeMovers.ST
        fuel = fuel_mapping[fuel_type_str]
        ramp_rate = sienna_match.maxRamp10 / 10.0
    else
        global unmatched_count += 1
        heat_rate     = LinearCurve(gen.COST_1)
        fuel_cost_ts  = fill(1.0, 8760)
        fuel          = ThermalFuels.NATURAL_GAS
        pm            = PrimeMovers.CT
        ramp_rate     = gen.RAMP_10 / 10.0
    end

    _add_thermal(sys, bus, name=acorn_name, fuel=fuel, heat_rate=heat_rate,
                 fuel_cost_ts=fuel_cost_ts, pmin=pmin, pmax=pmax, ramp_rate=ramp_rate, pm=pm,
                 ts_year=sim_year)
end
println("  Thermal: $(matched_count) matched, $(unmatched_count) unmatched")

# Hydro (component only — time series attached per year in the loop below)
println("Building hydro generators...")
for (_, hy) in enumerate(eachrow(df_acorn_hydro))
    bus = first(get_components(x -> PSY.get_number(x) == hy.GEN_BUS, ACBus, sys))
    op_cost = HydroGenerationCost(;
        variable=FuelCurve(; value_curve=LinearCurve(3.0), fuel_cost=1.0), fixed=0.0)
    device = PSY.HydroDispatch(
        name=hy.GEN_NAME,
        available=true,
        bus=bus,
        active_power=hy.PMAX / base_power,
        reactive_power=0.0,
        rating=hy.PMAX / base_power,
        prime_mover_type=PrimeMovers.HY,
        active_power_limits=PSY.MinMax((hy.PMIN / base_power, hy.PMAX / base_power)),
        reactive_power_limits=nothing,
        ramp_limits=(up=(hy.RAMP_10/10.0) / base_power, down=(hy.RAMP_10/10.0) / base_power),
        time_limits=(up=1.0, down=1.0),
        base_power=base_power,
        operation_cost=op_cost,
    )
    PSY.add_component!(sys, device)
end
println("  Added $(nrow(df_acorn_hydro)) hydro generators")

# Nuclear
println("Building nuclear generators...")
for (_, th) in enumerate(eachrow(df_acorn_nuclear))
    bus = first(get_components(x -> PSY.get_number(x) == th.GEN_BUS, ACBus, sys))
    op_cost = ThermalGenerationCost(;
        variable=FuelCurve(; value_curve=LinearCurve(1.1), fuel_cost=1.0),
        fixed=0.0, start_up=0.0, shut_down=0.0)
    _add_nuclear(sys, bus, name=th.GEN_NAME, fuel=ThermalFuels.NUCLEAR, cost=op_cost,
                 pmin=th.PMIN, pmax=th.PMAX, ramp_rate=th.RAMP_10/10.0, pm=PrimeMovers.ST)
end
println("  Added $(nrow(df_acorn_nuclear)) nuclear generators")

# Storage (capacity is static; no time series)
println("Building storage...")
for (_, sto) in enumerate(eachrow(df_storage))
    bus = first(get_components(x -> PSY.get_number(x) == Int(sto.bus_id), ACBus, sys))
    op_cost = StorageCost(charge_variable_cost=CostCurve(LinearCurve(0.0)))
    _add_storage(sys, bus, "Storage_bus_$(Int(sto.bus_id))",
                 sto.charge_capacity_MW, sto.storage_capacity_mwh, 0.95, op_cost)
end
println("  Added $(nrow(df_storage)) storage devices")

# AggGen (LMP-priced external imports — fuel cost time series uses fixed-year LMP)
println("Building aggregated external generators...")
for (_, th) in enumerate(eachrow(df_agg))
    bus = first(get_components(x -> PSY.get_number(x) == th.BusId, ACBus, sys))
    filtered_df = filter(row -> row.ZoneName == zonename_mapping[th.Zone], df_hourlylmp)
    zonal_price = filtered_df[1:8760, "LBMP"]
    _add_thermal(sys, bus, name=th.Name, fuel=ThermalFuels.OTHER,
                 heat_rate=LinearCurve(1.0, 0.0), fuel_cost_ts=zonal_price,
                 pmin=th.Pmin, pmax=th.Pmax, ramp_rate=th.maxRampAgc, pm=PrimeMovers.OT,
                 ts_year=sim_year)
end
println("  Added $(nrow(df_agg)) aggregated external generators")

println("Static grid complete.")
println("  Buses:          $(length(collect(get_components(ACBus, sys))))")
println("  Lines:          $(length(collect(get_components(Line, sys))))")
println("  Transformers:   $(length(collect(get_components(Transformer2W, sys))))")
println("  ThermalStandard:$(length(collect(get_components(ThermalStandard, sys))))")
println("  Storage:        $(length(collect(get_components(EnergyReservoirStorage, sys))))")

###############################################################################
# Attach time series for sim_year and serialize
###############################################################################

# --- Hydro ---
hydro_filepath = "$(acorn_input_dir)/large_hydro_$(climate_scenario).csv"
hydro_weekly = read_acorn_weekly_hydro(hydro_filepath, sim_year)
for (_, hy) in enumerate(eachrow(df_acorn_hydro))
    device = get_component(HydroDispatch, sys, hy.GEN_NAME)
    bus_id = hy.GEN_BUS
    hy_ts = if haskey(hydro_weekly, bus_id) || haskey(hydro_weekly, Float64(bus_id))
        get(hydro_weekly, bus_id, get(hydro_weekly, Float64(bus_id), nothing))
    else
        @warn "No weekly hydro data for $(hy.GEN_NAME) (bus $(bus_id)), using Pmax"
        ones(8760) * hy.PMAX
    end
    PSY.add_time_series!(sys, device,
        PSY.SingleTimeSeries("max_active_power",
            TimeArray(get_timestamp(sim_year), hy_ts / maximum(hy_ts)),
            scaling_factor_multiplier=PSY.get_max_active_power))
end
println("  Hydro time series attached")

# --- Wind ---
wind_filepath = "$(acorn_input_dir)/wind_$(climate_scenario).csv"
if isfile(wind_filepath)
    wind_df = read_acorn_timeseries(wind_filepath, sim_year)
    time_cols = names(wind_df)[2:end]
    wind_rows = [row for row in eachrow(wind_df) if maximum(Float64.([row[Symbol(col)] for col in time_cols])) > 0.0]
    for row in wind_rows
        bus_id = Int(row.bus_id)
        re_ts  = Float64.([row[Symbol(col)] for col in time_cols])
        bus = first(get_components(x -> PSY.get_number(x) == bus_id, ACBus, sys))
        op_cost = RenewableGenerationCost(; variable=CostCurve(; value_curve=LinearCurve(1.0)))
        _add_wind(sys, bus, "Wind_bus_$(bus_id)", maximum(re_ts), op_cost, re_ts, sim_year)
    end
    println("  Added $(length(wind_rows)) wind generators")
else
    println("  No wind file at $(wind_filepath), skipping")
end

# --- Solar UPV ---
solar_upv_filepath = "$(acorn_input_dir)/solar_upv_$(climate_scenario).csv"
if isfile(solar_upv_filepath)
    solar_df  = read_acorn_timeseries(solar_upv_filepath, sim_year)
    time_cols = names(solar_df)[2:end]
    solar_rows = [row for row in eachrow(solar_df) if maximum(Float64.([row[Symbol(col)] for col in time_cols])) > 0.0]
    for row in solar_rows
        bus_id = Int(row.bus_id)
        re_ts  = Float64.([row[Symbol(col)] for col in time_cols])
        bus = first(get_components(x -> PSY.get_number(x) == bus_id, ACBus, sys))
        op_cost = RenewableGenerationCost(; variable=CostCurve(; value_curve=LinearCurve(1.0)))
        _add_upv(sys, bus, "SolarUPV_bus_$(bus_id)", maximum(re_ts), op_cost, re_ts, sim_year)
    end
    println("  Added $(length(solar_rows)) solar UPV generators")
else
    println("  No solar UPV file at $(solar_upv_filepath), skipping")
end

# --- Load (with DPV and small hydro subtracted) ---
load_filepath = "$(acorn_input_dir)/load_$(climate_scenario).csv"
load_df = read_acorn_timeseries(load_filepath, sim_year)

solar_dpv_filepath = "$(acorn_input_dir)/solar_dpv_$(climate_scenario).csv"
if isfile(solar_dpv_filepath)
    load_df = subtract_from_load(load_df, read_acorn_timeseries(solar_dpv_filepath, sim_year))
    println("  Subtracted solar DPV from load")
end
small_hydro_filepath = "$(acorn_input_dir)/small_hydro_$(climate_scenario).csv"
if isfile(small_hydro_filepath)
    load_df = subtract_from_load(load_df, read_acorn_timeseries(small_hydro_filepath, sim_year))
    println("  Subtracted small hydro from load")
end

time_cols = names(load_df)[2:end]
for row in eachrow(load_df)
    bus_id  = Int(row.bus_id)
    load_ts = max.(Float64.([row[Symbol(col)] for col in time_cols]), 0.0)
    bus = first(get_components(x -> PSY.get_number(x) == bus_id, ACBus, sys))
    _build_load(sys, bus, "load_$(bus_id)", load_ts, sim_year)
end
println("  Added $(nrow(load_df)) loads")

# --- Serialize ---
out_file = "acorn_$(sim_year).json"
PSY.to_json(sys, out_file, force=true)
println("Saved $(out_file)")
println("SystemParsing complete for year $(sim_year).")
exit(0)
