DROP TABLE IF EXISTS par_inv_itf;

CREATE TABLE public.par_inv_itf (
    shelfname character varying(50),
    shelfHardware character varying(50),
    shelfNetworkRole character varying(25),
    shelfoperationalstate character varying(20),
    portInterfaceName character varying(20),
    portBandwidth character varying(20),
    portoperationalstate character varying(20),
    portInfo1 character varying(500),
    portType character varying(20),
    concat character varying(90)
);


ALTER TABLE public.par_inv_itf OWNER TO airflow;

DROP TABLE IF EXISTS inv_itf;

CREATE TABLE public.inv_itf (
    shelfname character varying(50),
    shelfHardware character varying(50),
    shelfNetworkRole character varying(25),
    shelfoperationalstate character varying(20),
    portInterfaceName character varying(20),
    portBandwidth character varying(20),
    portoperationalstate character varying(20),
    portInfo1 character varying(500),
    portType character varying(20)
);


ALTER TABLE public.inv_itf OWNER TO airflow;


DROP TABLE IF EXISTS ne;

CREATE TABLE public.ne (
    shelfname character varying(50),
    interface character varying(20),
    portoperationalstate character varying(20),
    protocol character varying(20),
    info1 character varying(500),
    concat character varying(90)
);


ALTER TABLE public.ne OWNER TO airflow;


DROP TABLE IF EXISTS ansible_history;

CREATE TABLE public.ansible_history (
    ne character varying(30)
);

ALTER TABLE public.ansible_history OWNER TO airflow;