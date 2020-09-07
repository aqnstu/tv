-- Table: vacs.tv_params

-- DROP TABLE vacs.tv_params;

CREATE TABLE vacs.tv_params
(
    id integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    similarity_level_mrigo integer NOT NULL,
    similarity_level_okpdtr integer NOT NULL,
    date_add timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT tv_params_pkey PRIMARY KEY (id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE vacs.tv_params
    OWNER to dba;