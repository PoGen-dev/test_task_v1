-- Table: public.new_projects

-- DROP TABLE IF EXISTS public.new_projects;

CREATE TABLE IF NOT EXISTS public.new_projects
(
    id integer NOT NULL DEFAULT nextval('new_projects_id_seq'::regclass),
    code character varying(255) COLLATE pg_catalog."default",
    project character varying(255) COLLATE pg_catalog."default",
    year integer,
    value double precision,
    version integer,
    CONSTRAINT new_projects_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.new_projects
    OWNER to postgres;