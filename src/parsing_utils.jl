using PowerSystems
const PSY = PowerSystems

#Function to generate exactly 8760 hourly timestamps starting Jan 1 of the given year
get_timestamp(year) = DateTime("$(year)-01-01T00:00:00"):Hour(1):(DateTime("$(year)-01-01T00:00:00") + Hour(8759))


###############################################################################
# Acorn CSV readers - read wide-format acorn CSVs and filter by year
###############################################################################

"""
    read_acorn_timeseries(filepath, sim_year)

Read an acorn wide-format CSV (bus_id column + datetime columns) and filter
to the specified simulation year. Returns a DataFrame with bus_id and hourly columns.
"""
function read_acorn_timeseries(filepath, sim_year)
    df = CSV.read(filepath, DataFrame)
    date_cols = names(df)[2:end]
    # Acorn datetime columns use format: "yyyy-mm-dd HH:MM:SS+00:00"
    year_cols = filter(date_cols) do col
        try
            dt = DateTime(replace(col, r"\+00:00$" => ""), "yyyy-mm-dd HH:MM:SS")
            return year(dt) == sim_year
        catch
            # Try date-only format for weekly data
            try
                dt = Date(col, "yyyy-mm-dd")
                return year(dt) == sim_year
            catch
                return false
            end
        end
    end
    # Sienna uses a non-leap reference year (2019), so truncate to 8760 hours
    if length(year_cols) > 8760
        year_cols = year_cols[1:8760]
    end
    return select(df, [:bus_id; Symbol.(year_cols)])
end

"""
    read_acorn_weekly_hydro(filepath, sim_year)

Read acorn large hydro weekly energy CSV and convert to hourly constant power.
Returns a Dict mapping bus_id => Vector{Float64} of 8760 hourly power values (MW).
"""
function read_acorn_weekly_hydro(filepath, sim_year)
    df = CSV.read(filepath, DataFrame)
    date_cols = names(df)[2:end]
    # Large hydro has weekly dates in "yyyy-mm-dd" format
    year_cols = filter(date_cols) do col
        try
            dt = Date(col, "yyyy-mm-dd")
            return year(dt) == sim_year
        catch
            return false
        end
    end
    filtered = select(df, [:bus_id; Symbol.(year_cols)])

    result = Dict{Any,Vector{Float64}}()
    n_weeks = length(year_cols)
    hours_in_year = 8760
    full_week_hours = 168  # 7 * 24

    for row in eachrow(filtered)
        bus_id = row.bus_id
        weekly_energy = [row[Symbol(col)] for col in year_cols]
        hourly_power = Float64[]
        for w in 1:n_weeks
            # Always divide by full_week_hours (168) regardless of how many hours
            # remain in the year. The last partial week's energy represents a full
            # week's budget; dividing by fewer hours inflates the power artificially.
            power = weekly_energy[w] / full_week_hours  # MWh / 168 h = MW
            h = w < n_weeks ? full_week_hours : hours_in_year - (n_weeks - 1) * full_week_hours
            append!(hourly_power, fill(power, h))
        end
        # Trim or pad to exactly 8760
        if length(hourly_power) > hours_in_year
            hourly_power = hourly_power[1:hours_in_year]
        elseif length(hourly_power) < hours_in_year
            append!(hourly_power, fill(hourly_power[end], hours_in_year - length(hourly_power)))
        end
        result[bus_id] = hourly_power
    end
    return result
end

"""
    subtract_from_load(load_df, subtract_df)

Subtract generation (DPV or small hydro) from load DataFrame.
Both DataFrames should already be filtered to the same year.
Modifies load in-place by subtracting matching bus generation.
"""
function subtract_from_load(load_df, subtract_df)
    load = copy(load_df)
    # Get common time columns (everything except bus_id)
    load_cols = Set(names(load))
    sub_cols = Set(names(subtract_df))
    common_cols = intersect(load_cols, sub_cols)
    delete!(common_cols, "bus_id")

    if isempty(common_cols)
        @warn "No matching time columns between load and subtraction data"
        return load
    end

    time_cols = sort(collect(common_cols))

    for row in eachrow(subtract_df)
        bus_id = row.bus_id
        bus_idx = findfirst(==(bus_id), load.bus_id)
        if bus_idx !== nothing
            for col in time_cols
                load[bus_idx, Symbol(col)] -= row[Symbol(col)]
            end
        end
    end
    return load
end

"""
    strip_acorn_name(name)

Strip the parenthesized PTID suffix from acorn generator names.
E.g. "Danskammer 1(2480,1)" -> "Danskammer 1"
"""
strip_acorn_name(name) = strip(replace(name, r"\(.*\)$" => ""))

"""
    match_thermal_generators(acorn_ng_df, sienna_thermal_df)

Match acorn NG generators to nygrid2sienna thermal_config by name.
Returns a Dict mapping acorn row index => sienna thermal row (or nothing if unmatched).
"""
function match_thermal_generators(acorn_ng_df, sienna_thermal_df)
    matches = Dict{Int,Any}()
    sienna_names = sienna_thermal_df.Name
    for (i, row) in enumerate(eachrow(acorn_ng_df))
        acorn_name = strip_acorn_name(row.GEN_NAME)
        idx = findfirst(==(acorn_name), sienna_names)
        if idx !== nothing
            matches[i] = sienna_thermal_df[idx, :]
        else
            matches[i] = nothing
        end
    end
    return matches
end


###############################################################################
# Grid builder functions (from nygrid2sienna/clcpa2040)
###############################################################################

function _build_bus(sys, number, name, bustype, angle, magnitude, voltage_limits, base_voltage, area; available=true)
    bus = PSY.ACBus(
        number=number,
        name=name,
        bustype=bustype,
        angle=angle,
        magnitude=magnitude,
        voltage_limits=voltage_limits,
        base_voltage=base_voltage,
        area=area,
        available=available,
    )
    add_component!(sys, bus)
end

function _build_lines(sys; frombus::PSY.ACBus, tobus::PSY.ACBus, name, r, x, b, rating)
    device = PSY.Line(
        name=name,
        available=true,
        active_power_flow=rating / 100.0,
        reactive_power_flow=0.0,
        arc=PSY.Arc(from=frombus, to=tobus),
        r=r,
        x=x,
        b=(from=b, to=b),
        rating=rating / 100.0,
        angle_limits=PSY.MinMax((-1.571 * 2, 1.571 * 2)),
    )
    PSY.add_component!(sys, device)
    return device
end


function _build_transformers(sys; frombus::PSY.ACBus, tobus::PSY.ACBus, name, r, x, b, rating)
    device = PSY.Transformer2W(
        name=name,
        available=true,
        active_power_flow=rating / 100.0,
        reactive_power_flow=0.0,
        arc=PSY.Arc(from=frombus, to=tobus),
        r=r,
        x=x,
        primary_shunt=b,
        rating=rating / 100.0,
        base_power=100.0,
    )
    PSY.add_component!(sys, device)
    return device
end


function _build_hvdc(sys; frombus::PSY.ACBus, tobus::PSY.ACBus, name, r, x, b, rating)
    device = PSY.TwoTerminalHVDCLine(
        name=name,
        available=true,
        active_power_flow=rating / base_power,
        arc=PSY.Arc(from=frombus, to=tobus),
        active_power_limits_from=PSY.MinMax((-rating / base_power, rating / base_power)),
        active_power_limits_to=PSY.MinMax((-rating / base_power, rating / base_power)),
        reactive_power_limits_from=PSY.MinMax((-rating / base_power, rating / base_power)),
        reactive_power_limits_to=PSY.MinMax((-rating / base_power, rating / base_power)),
        loss=LinearCurve(0.0),
    )
    PSY.add_component!(sys, device)
    return device
end


function _build_interface_flow(sys; name, rating_lb, rating_ub, ifdict)
    service = PSY.TransmissionInterface(
        name=name,
        available=true,
        active_power_flow_limits=PSY.MinMax((rating_lb / 100.0, rating_ub / 100.0)),
        violation_penalty=0.0,
        direction_mapping=ifdict
    )
    contri_devices = PSY.get_components(
        x -> haskey(ifdict, PSY.get_name(x)),
        ACBranch,
        sys,
    )
    PSY.add_service!(sys, service, contri_devices)
    return service
end


###############################################################################
# Generator builder functions
###############################################################################

function _add_thermal(
    sys,
    bus::PSY.Bus;
    name,
    fuel::PSY.ThermalFuels,
    pmin,
    pmax,
    ramp_rate,
    heat_rate::PSY.LinearCurve,
    fuel_cost_ts::Vector,
    pm::PSY.PrimeMovers,
    ts_year::Int,
)
    # Create device with placeholder fuel_cost=1.0
    device = PSY.ThermalStandard(
        name=name,
        available=true,
        status=true,
        bus=bus,
        active_power=0.0,
        reactive_power=0.0,
        rating=pmax / base_power,
        active_power_limits=PSY.MinMax((pmin / base_power, pmax / base_power)),
        reactive_power_limits=nothing,
        ramp_limits=(up=ramp_rate / base_power, down=ramp_rate / base_power),
        operation_cost=ThermalGenerationCost(
            variable=FuelCurve(; value_curve=heat_rate, fuel_cost=1.0),
            fixed=0.0,
            start_up=0.0,
            shut_down=0.0),
        base_power=base_power,
        time_limits=(up=1.0, down=1.0),
        prime_mover_type=pm,
        fuel=fuel,
        time_at_status=999.0,
        ext=Dict{String,Any}(),
    )
    PSY.add_component!(sys, device)

    # Attach hourly fuel cost time series
    PSY.add_time_series!(
        sys,
        device,
        PSY.SingleTimeSeries(
            "fuel_cost",
            TimeArray(get_timestamp(ts_year), fuel_cost_ts),
            scaling_factor_multiplier=nothing,
        )
    )

    # Update operation cost to reference the time series key
    fuel_curve = FuelCurve(; value_curve=heat_rate, fuel_cost=get_time_series_keys(device)[1])
    set_operation_cost!(device, ThermalGenerationCost(;
        variable=fuel_curve,
        fixed=0.0,
        start_up=0.0,
        shut_down=0.0,
    ))
    return device
end

function _add_fuel_cost(heatrate1, heatrate0, zone, fuel, pmin, priceTable)
    heat_rate_curve = LinearCurve(heatrate1, heatrate0)
    if fuel == "Coal"
        fuelPrice = priceTable[!, "coal_NY"]
    elseif fuel == "Natural Gas"
        fuelPrice = priceTable[!, "NG_A2E"]
        if zone in ["F", "G", "H", "I"]
            fuelPrice = priceTable[!, "NG_F2I"]
        end
        if zone == "K"
            fuelPrice = priceTable[!, "NG_J"]
        end
        if zone == "J"
            fuelPrice = priceTable[!, "NG_K"]
        end
    elseif fuel == "Fuel Oil 2" || fuel == "Kerosene"
        if zone in ["F", "G", "H", "I"]
            fuelPrice = priceTable[!, "FO2_DSNY"]
        else
            fuelPrice = priceTable[!, "FO2_UPNY"]
        end
    elseif fuel == "Fuel Oil 6"
        if zone in ["F", "G", "H", "I"]
            fuelPrice = priceTable[!, "FO6_DSNY"]
        else
            fuelPrice = priceTable[!, "FO6_UPNY"]
        end
    else
        error("Error: Undefined fuel type!")
    end
    return heat_rate_curve, fuelPrice
end

function _add_nuclear(
    sys,
    bus::PSY.Bus;
    name,
    fuel::PSY.ThermalFuels,
    pmin,
    pmax,
    ramp_rate,
    cost::PSY.OperationalCost,
    pm::PSY.PrimeMovers,
)
    device = PSY.ThermalStandard(
        name=name,
        available=true,
        status=true,
        bus=bus,
        active_power=0.0,
        reactive_power=0.0,
        rating=pmax / base_power,
        active_power_limits=PSY.MinMax((pmin / base_power, pmax / base_power)),
        reactive_power_limits=nothing,
        ramp_limits=(up=ramp_rate / base_power, down=ramp_rate / base_power),
        operation_cost=cost,
        base_power=base_power,
        time_limits=(up=1.0, down=1.0),
        prime_mover_type=pm,
        fuel=fuel,
        time_at_status=999.0,
        ext=Dict{String,Any}(),
    )
    PSY.add_component!(sys, device)
    return device
end

function _add_hydro(
    sys,
    bus::PSY.Bus;
    name,
    pmin,
    pmax,
    ramp_rate,
    cost::PSY.OperationalCost,
    pm::PSY.PrimeMovers,
    ts,
    ts_year::Int,
)
    device = PSY.HydroDispatch(
        name=name,
        available=true,
        bus=bus,
        active_power=pmax / base_power,
        reactive_power=0.0,
        rating=pmax / base_power,
        prime_mover_type=pm,
        active_power_limits=PSY.MinMax((pmin / base_power, pmax / base_power)),
        reactive_power_limits=nothing,
        ramp_limits=(up=ramp_rate / base_power, down=ramp_rate / base_power),
        time_limits=(up=1.0, down=1.0),
        base_power=base_power,
        operation_cost=cost,)

    PSY.add_component!(sys, device)
    PSY.add_time_series!(
        sys,
        device,
        PSY.SingleTimeSeries(
            "max_active_power",
            TimeArray(get_timestamp(ts_year), ts / maximum(ts)),
            scaling_factor_multiplier=PSY.get_max_active_power,
        )
    )
    return device
end

function _add_wind(sys, bus::PSY.Bus, name, rating, op_cost, re_ts, load_year)
    wind = PSY.RenewableDispatch(
        name=name,
        available=true,
        bus=bus,
        active_power=rating / 100.0,
        reactive_power=0.0,
        rating=rating / 100.0,
        prime_mover_type=PSY.PrimeMovers.WT,
        reactive_power_limits=(min=0.0, max=1.0 * rating / 100.0),
        power_factor=1.0,
        operation_cost=op_cost,
        base_power=100,
    )
    add_component!(sys, wind)

    PSY.add_time_series!(
        sys,
        wind,
        PSY.SingleTimeSeries(
            "max_active_power",
            TimeArray(get_timestamp(load_year), re_ts / maximum(re_ts)),
            scaling_factor_multiplier=PSY.get_max_active_power,
        )
    )
    return wind
end

function _add_upv(sys, bus::PSY.Bus, name, rating, op_cost, re_ts, load_year)
    solar = PSY.RenewableDispatch(
        name=name,
        available=true,
        bus=bus,
        active_power=rating / 100.0,
        reactive_power=0.0,
        rating=rating / 100.0,
        prime_mover_type=PSY.PrimeMovers.PVe,
        reactive_power_limits=(min=0.0, max=1.0 * rating / 100.0),
        power_factor=1.0,
        operation_cost=op_cost,
        base_power=100,
    )
    add_component!(sys, solar)

    PSY.add_time_series!(
        sys,
        solar,
        PSY.SingleTimeSeries(
            "max_active_power",
            TimeArray(get_timestamp(load_year), re_ts / rating),
            scaling_factor_multiplier=PSY.get_max_active_power,
        )
    )

    return solar
end

function _add_storage(sys, bus::PSY.Bus, name, power_capacity, energy_capacity, efficiency, op_cost)
    device = PSY.EnergyReservoirStorage(
        name=name,
        available=true,
        bus=bus,
        prime_mover_type=PSY.PrimeMovers.BA,
        storage_technology_type=StorageTech.LIB,
        storage_capacity=energy_capacity / 100.0,
        storage_level_limits=(min=0.1, max=1.0),
        initial_storage_capacity_level=0.5,
        rating=power_capacity / 100.0,
        active_power=power_capacity / 100.0,
        input_active_power_limits=(min=0.0, max=power_capacity / 100.0),
        output_active_power_limits=(min=0.0, max=power_capacity / 100.0),
        efficiency=(in=efficiency, out=1.0),
        reactive_power=0.0,
        reactive_power_limits=nothing,
        base_power=100.0,
        operation_cost=op_cost
    )
    PSY.add_component!(sys, device)
    return device
end

function _build_load(sys, bus::PSY.Bus, name, load_ts, load_year)
    if maximum(load_ts) == 0.0
        maxload = minimum(load_ts) / base_power
    else
        maxload = maximum(load_ts) / base_power
    end
    load = PSY.StandardLoad(
        name=name,
        available=true,
        bus=bus,
        base_power=100.0,
        max_constant_active_power=maxload,
    )
    add_component!(sys, load)

    if maximum(load_ts) == 0.0 && minimum(load_ts) == 0.0
        PSY.add_time_series!(
            sys,
            load,
            PSY.SingleTimeSeries(
                "max_active_power",
                TimeArray(get_timestamp(load_year), load_ts),
                scaling_factor_multiplier=PSY.get_max_active_power,
            )
        )
    elseif maximum(load_ts) == 0.0
        PSY.add_time_series!(
            sys,
            load,
            PSY.SingleTimeSeries(
                "max_active_power",
                TimeArray(get_timestamp(load_year), load_ts / minimum(load_ts)),
                scaling_factor_multiplier=PSY.get_max_active_power,
            )
        )
    else
        PSY.add_time_series!(
            sys,
            load,
            PSY.SingleTimeSeries(
                "max_active_power",
                TimeArray(get_timestamp(load_year), load_ts / maximum(load_ts)),
                scaling_factor_multiplier=PSY.get_max_active_power,
            )
        )
    end

    return load
end


function add_reserves(sys; reg_reserve_frac=0.05, spinning_reserve_frac=0.1)
    PSY.set_units_base_system!(sys, UnitSystem.NATURAL_UNITS)
    power_loads = get_components(StandardLoad, sys)
    ts_length = length(get_time_series_values(SingleTimeSeries, collect(power_loads)[1], "max_active_power"))
    reserve_ts = zeros(ts_length)
    TS = zeros(ts_length)

    for p in power_loads
        ts = get_time_series_values(SingleTimeSeries, p, "max_active_power")
        reserve_ts = reserve_ts .+ ts * reg_reserve_frac
        TS = get_time_series_timestamps(SingleTimeSeries, p, "max_active_power")
    end
    service = PSY.VariableReserve{ReserveUp}(
        name="Reg_Up",
        available=true,
        time_frame=60,
        requirement=maximum(reserve_ts) / 100,
        deployed_fraction=0.0,
        max_participation_factor=0.5,
        max_output_fraction=0.5,
        sustained_time=3600.0)
    contri_devices = get_components(x -> !(typeof(x) <: StaticLoad), StaticInjection, sys)
    add_service!(sys, service, contri_devices)
    add_time_series!(
        sys,
        service,
        SingleTimeSeries("requirement", TimeArray(TS, reserve_ts ./ maximum(reserve_ts)), scaling_factor_multiplier=get_requirement)
    )

    spin_reserve_ts = zeros(ts_length)
    if length(get_components(x -> PSY.get_prime_mover_type(x) == PSY.PrimeMovers.PVe && PSY.get_available(x) == true, PSY.RenewableGen, sys)) > 0
        if length(get_components(x -> PSY.get_prime_mover_type(x) == PSY.PrimeMovers.WT && PSY.get_available(x) == true, PSY.RenewableGen, sys)) > 0
            TS_spin = get_time_series_timestamps(SingleTimeSeries, first(get_components(x -> PSY.get_available(x) == true, PSY.RenewableGen, sys)), "max_active_power")
            for p in get_components(x -> PSY.get_available(x) == true, PSY.RenewableGen, sys)
                ts = get_time_series_values(SingleTimeSeries, p, "max_active_power")
                spin_reserve_ts = spin_reserve_ts .+ ts * spinning_reserve_frac
            end
        else
            TS_spin = get_time_series_timestamps(SingleTimeSeries, first(get_components(x -> PSY.get_prime_mover_type(x) == PSY.PrimeMovers.PVe && PSY.get_available(x) == true, PSY.RenewableGen, sys)), "max_active_power")
            for p in get_components(x -> PSY.get_prime_mover_type(x) == PSY.PrimeMovers.PVe && PSY.get_available(x) == true, PSY.RenewableGen, sys)
                ts = get_time_series_values(SingleTimeSeries, p, "max_active_power")
                spin_reserve_ts = spin_reserve_ts .+ ts * spinning_reserve_frac
            end
        end

        service = PSY.VariableReserve{ReserveUp}(
            name="Flex_Up",
            available=true,
            time_frame=60.0,
            requirement=maximum(spin_reserve_ts) / 100,
            deployed_fraction=0.0,
            max_participation_factor=0.5,
            max_output_fraction=1.0,
            sustained_time=3600.0)
        contri_devices = get_components(x -> !(typeof(x) <: StaticLoad) && !(typeof(x) <: RenewableGen), StaticInjection, sys)
        add_service!(sys, service, contri_devices)
        add_time_series!(
            sys,
            service,
            SingleTimeSeries("requirement", TimeArray(TS_spin, spin_reserve_ts ./ maximum(spin_reserve_ts)), scaling_factor_multiplier=get_requirement)
        )
        PSY.set_units_base_system!(sys, UnitSystem.SYSTEM_BASE)
    end
    return sys
end
