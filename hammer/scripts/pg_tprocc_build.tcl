#!/bin/tclsh
# PostgreSQL TPC-C Schema Build Script for GX POC
# This script creates a TPC-C database schema for testing Great Expectations

puts "=== HammerDB PostgreSQL TPC-C Schema Build ==="
puts ""

# Database configuration
dbset db pg
dbset bm TPC-C

# Get connection settings from environment variables
set pg_host $::env(PG_HOST)
set pg_port $::env(PG_PORT)
set pg_user $::env(PG_USER)
set pg_password $::env(PG_PASSWORD)
set pg_database $::env(PG_DATABASE)
set warehouse_count $::env(WAREHOUSE_COUNT)
set num_vu $::env(NUM_VU)

puts "Connection Settings:"
puts "  Host: $pg_host"
puts "  Port: $pg_port"
puts "  Database: $pg_database"
puts "  Warehouses: $warehouse_count"
puts "  Virtual Users: $num_vu"
puts ""

# Set connection parameters
diset connection pg_host $pg_host
diset connection pg_port $pg_port

# Schema configuration - use the main postgres user
diset tpcc pg_superuser $pg_user
diset tpcc pg_superuserpass $pg_password
diset tpcc pg_defaultdbase postgres

# TPC-C specific database and user
diset tpcc pg_dbase $pg_database
diset tpcc pg_user $pg_user
diset tpcc pg_pass $pg_password

# Build parameters
diset tpcc pg_count_ware $warehouse_count
diset tpcc pg_num_vu $num_vu

# Schema options
diset tpcc pg_storedprocs true
diset tpcc pg_partition false
diset tpcc pg_oracompat false

# Print final configuration
puts "Building schema..."
print dict

# Build schema
puts ""
puts "Starting schema creation and data load..."
buildschema

# Wait for build to complete
puts "Waiting for schema build to complete..."

proc wait_to_complete {} {
    global complete
    set complete [vucomplete]
    if {!$complete} {
        after 5000 wait_to_complete
    } else {
        puts ""
        puts "Schema build completed successfully!"
    }
}

wait_to_complete

# Clean up virtual users
vudestroy

puts ""
puts "=== TPC-C Database Ready ==="
puts "Tables created: warehouse, district, customer, history, orders,"
puts "                new_order, order_line, stock, item"
puts ""
