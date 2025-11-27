-- ======================================================================
-- 1. MMV MASTER (VEHICLE MAKE, MODEL, VARIANT)
-- Purpose: Stores all vehicle configurations (2W/4W) and insurer mappings.
-- ======================================================================

CREATE TABLE mmv_master (
    -- Core Identity
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), -- Automatically generated UUID
    product_id INTEGER NOT NULL,                 -- 1 = 2W, 2 = 4W
    
    -- Business Keys
    make VARCHAR(100) NOT NULL,
    model VARCHAR(255) NOT NULL,
    variant VARCHAR(255) NOT NULL,
    ensuredit_id VARCHAR(50) UNIQUE,             -- UNIQUE KEY for ON CONFLICT clause
    
    -- Technical Specs
    cc INTEGER,
    fuelType VARCHAR(50),
    body_type VARCHAR(50),
    seating_capacity INTEGER,
    carrying_capacity INTEGER,

    -- Insurer Mappings (Stored as TEXT/JSON string)
    digit TEXT, icici TEXT, hdfc TEXT, reliance TEXT, bajaj TEXT, tata TEXT, sbi TEXT, 
    future TEXT, iffco TEXT, chola TEXT, royalSundaram TEXT, zuno TEXT, kotak TEXT, 
    acko TEXT, magma TEXT, united TEXT
);

-- Note: The UNIQUE constraint on ensuredit_id is necessary for ON CONFLICT.

-- ======================================================================
-- 2. RTO MASTER (REGIONAL TRANSPORT OFFICE)
-- Purpose: Stores RTO location data and insurer RTO/Region mappings.
-- ======================================================================

CREATE TABLE rto_master (
    -- Core Identity (Primary Key, unique RTO ID from CSV)
    id VARCHAR(50) PRIMARY KEY,
    
    -- Location Details
    rto VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(100),
    search_string TEXT,
    display_string TEXT,
    
    -- Insurer Mappings (Stored as TEXT/JSON string)
    chola TEXT, tata TEXT, iffco TEXT, icici TEXT, sbi TEXT, bajaj TEXT, 
    reliance TEXT, hdfc TEXT, future TEXT, zuno TEXT, kotak TEXT, magma TEXT, 
    united TEXT, royalSundaram TEXT, shriram TEXT, digit TEXT, acko TEXT
);

-- ======================================================================
-- 3. PINCODE MASTER
-- Purpose: Stores Pincode location data and insurer health/motor mappings.
-- ======================================================================

CREATE TABLE pincode_master (
    -- Core Identity
    pincode VARCHAR(50) PRIMARY KEY,
    
    -- Location Details
    district VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    
    -- Insurer Mappings (Includes Health and Motor Insurers)
    icici TEXT, digit TEXT, reliance TEXT, hdfc TEXT, bajaj TEXT, tata TEXT, 
    sbi TEXT, future TEXT, iffco TEXT, chola TEXT, kotak TEXT, acko TEXT, 
    magma TEXT, zuno TEXT, royalSundaram TEXT, united TEXT, shriram TEXT,
    care TEXT, cigna TEXT, hdfclife TEXT, tataaia TEXT, hdfchealth TEXT, 
    carecashless TEXT, nivabupa TEXT, cholapa TEXT, oic TEXT, tatamhg TEXT, 
    icicihealth TEXT
);