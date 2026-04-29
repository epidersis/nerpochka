create schema if not exists raw;
create schema if not exists stg;
create schema if not exists mart;

create table if not exists raw.import_files (
    id bigserial primary key,
    folder_name text not null,
    file_name text not null,
    file_path text not null,
    file_hash text not null unique,
    file_type text,
    period_date date,
    uploaded_at timestamp default now(),
    status text default 'new',
    rows_count int default 0
);

create table if not exists raw.csv_rows (
    id bigserial primary key,
    import_file_id bigint references raw.import_files(id),
    row_number int,
    data jsonb not null
);

create table if not exists stg.budget_operations (
    id bigserial primary key,
    import_file_id bigint references raw.import_files(id),
    period_from date,
    period_to date,
    documentclass_id text,
    budget_id text,
    document_id text,
    amount numeric(18,2),
    estimate_name text,
    recipient_name text,
    kadmr_code text,
    kfsr_code text,
    kcsr_code text,
    kvr_code text,
    kesr_code text,
    kdr_code text,
    kde_code text,
    kdf_code text
);

create table if not exists stg.gz_budget_lines (
    id bigserial primary key,
    import_file_id bigint references raw.import_files(id),
    con_document_id text,
    kfsr_code text,
    kcsr_code text,
    kvr_code text,
    kesr_code text,
    kdr_code text,
    kde_code text,
    kdf_code text,
    amount numeric(18,2)
);

create table if not exists stg.gz_contracts (
    id bigserial primary key,
    import_file_id bigint references raw.import_files(id),
    con_document_id text,
    con_number text,
    con_date date,
    customer_name text,
    supplier_name text,
    con_amount numeric(18,2)
);

create table if not exists stg.gz_payments (
    id bigserial primary key,
    import_file_id bigint references raw.import_files(id),
    con_document_id text,
    payment_id text,
    payment_date date,
    payment_amount numeric(18,2)
);

create table if not exists stg.agreements (
    id bigserial primary key,
    import_file_id bigint references raw.import_files(id),
    period_from date,
    period_to date,
    documentclass_id text,
    budget_id text,
    document_id text,
    amount numeric(18,2),
    agreement_number text,
    estimate_name text,
    recipient_name text,
    kadmr_code text,
    kfsr_code text,
    kcsr_code text,
    kvr_code text,
    kesr_code text,
    kdr_code text,
    kde_code text,
    kdf_code text
);

create table if not exists stg.buau_operations (
    id bigserial primary key,
    import_file_id bigint references raw.import_files(id),
    operation_date date,
    budget_name text,
    organization_name text,
    provider_name text,
    amount numeric(18,2),
    amount_with_refund numeric(18,2),
    refund_amount numeric(18,2),
    kfsr_code text,
    kcsr_code text,
    kvr_code text,
    kesr_code text,
    kdr_code text,
    kdr_name text,
    kde_code text,
    kdf_code text
);

create table if not exists mart.indicators (
    id bigserial primary key,
    source_type text not null,
    source_file_id bigint references raw.import_files(id),

    period_from date,
    period_to date,

    section text,
    object_code text,
    object_name text,

    kcsr_code text,
    kvr_code text,
    kfsr_code text,
    kadmr_code text,
    kesr_code text,
    kdr_code text,
    kde_code text,
    kdf_code text,

    indicator_type text not null,
    amount numeric(18,2) not null default 0,

    document_id text,
    contract_id text,
    payment_id text,
    recipient_name text,
    contractor_name text
);

create index if not exists idx_mart_kcsr on mart.indicators(kcsr_code);
create index if not exists idx_mart_section on mart.indicators(section);
create index if not exists idx_mart_indicator on mart.indicators(indicator_type);
create index if not exists idx_mart_period on mart.indicators(period_to);
