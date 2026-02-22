-- Migration: Create Twilio Accounts Table
-- Purpose: Store encrypted Twilio subaccount credentials for each user
-- Date: 2026-02-14

-- Create twilio_accounts table
CREATE TABLE IF NOT EXISTS twilio_accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES auth.users(id) ON DELETE CASCADE NOT NULL,
    subaccount_sid TEXT NOT NULL UNIQUE,
    encrypted_auth_token TEXT NOT NULL,
    status TEXT DEFAULT 'active' CHECK (status IN ('active', 'suspended', 'deleted')),
    friendly_name TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Ensure one subaccount per user
    CONSTRAINT unique_user_subaccount UNIQUE (user_id)
);

-- Create index for fast user lookups
CREATE INDEX IF NOT EXISTS idx_twilio_accounts_user_id ON twilio_accounts(user_id);

-- Create index for subaccount SID lookups
CREATE INDEX IF NOT EXISTS idx_twilio_accounts_sid ON twilio_accounts(subaccount_sid);

-- Enable Row Level Security
ALTER TABLE twilio_accounts ENABLE ROW LEVEL SECURITY;

-- Policy: Users can view their own Twilio account
CREATE POLICY "Users can view their own Twilio account"
    ON twilio_accounts FOR SELECT
    USING (auth.uid() = user_id);

-- Policy: Service role can manage all Twilio accounts
CREATE POLICY "Service role can manage all Twilio accounts"
    ON twilio_accounts FOR ALL
    USING (auth.role() = 'service_role');

-- Create updated_at trigger
CREATE OR REPLACE FUNCTION update_twilio_accounts_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_twilio_accounts_updated_at
    BEFORE UPDATE ON twilio_accounts
    FOR EACH ROW
    EXECUTE FUNCTION update_twilio_accounts_updated_at();

-- Add comment for documentation
COMMENT ON TABLE twilio_accounts IS 'Stores encrypted Twilio subaccount credentials for auto-provisioned user accounts';
COMMENT ON COLUMN twilio_accounts.encrypted_auth_token IS 'Fernet-encrypted authentication token, never stored in plaintext';
