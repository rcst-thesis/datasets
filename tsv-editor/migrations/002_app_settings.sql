-- ── TSV Editor — App Settings (global shared state) ─────────────────────────
-- Stores custom dictionary and dialect extras shared across all annotators.

create table if not exists app_settings (
    key         text        primary key,
    value       jsonb       not null default 'null'::jsonb,
    updated_at  timestamptz not null default now()
);

-- Self-contained updated_at trigger (no dependency on 001's functions)
create or replace function _settings_set_updated_at()
returns trigger language plpgsql as $$
begin
    new.updated_at = now();
    return new;
end;
$$;

drop trigger if exists trg_app_settings_updated_at on app_settings;
create trigger trg_app_settings_updated_at
    before update on app_settings
    for each row execute function _settings_set_updated_at();

alter table app_settings enable row level security;

drop policy if exists "allow_all_app_settings" on app_settings;
create policy "allow_all_app_settings"
    on app_settings for all using (true) with check (true);

-- Seed empty structures so upsert never fails on first read
insert into app_settings (key, value) values
    ('custom_dict',   '[]'::jsonb),
    ('dialect_extra', '[]'::jsonb)
on conflict (key) do nothing;