-- Table: vacs.tv_log

-- DROP TABLE vacs.tv_log;

CREATE TABLE vacs.tv_log
(
    id integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    exit_point integer,
    message character varying COLLATE pg_catalog."default" NOT NULL,
    num_of_companies integer,
    num_of_vacancies integer,
    date_add timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT tv_loq_pkey PRIMARY KEY (id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE vacs.tv_log
    OWNER to dba;