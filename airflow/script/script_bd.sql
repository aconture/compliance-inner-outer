# docker exec -it <docker postgres> sh
# psql -U airflow


DROP TABLE par_inv_itf;

DROP TABLE inv_itf;

DROP TABLE ne;

DROP TABLE core_history;

CREATE TABLE public.par_inv_itf (
    shelfname character varying(50),
    shelfHardware character varying(30),
    shelfNetworkRole character varying(25),
    shelfoperationalstate character varying(10),
    portInterfaceName character varying(20),
    portBandwidth character varying(20),
    portoperationalstate character varying(20),
    portInfo1 character varying(500),
    portType character varying(20),
    concat character varying(90)
);


ALTER TABLE public.par_inv_itf OWNER TO airflow;

CREATE TABLE public.inv_itf (
    shelfname character varying(50),
    shelfHardware character varying(30),
    shelfNetworkRole character varying(25),
    shelfoperationalstate character varying(10),
    portInterfaceName character varying(20),
    portBandwidth character varying(20),
    portoperationalstate character varying(20),
    portInfo1 character varying(500),
    portType character varying(20)
);


ALTER TABLE public.inv_itf OWNER TO airflow;

CREATE TABLE public.ne (
    shelfname character varying(50),
    interface character varying(20),
    portoperationalstate character varying(20),
    protocol character varying(20),
    info1 character varying(500),
    concat character varying(90)
);


ALTER TABLE public.ne OWNER TO airflow;


CREATE TABLE public.core_history (
    ne character varying(30),
	ok character varying(5),
	revisar character varying(5),
	finv character varying(5),
	fecha character varying(20)
);


ALTER TABLE public.core_history OWNER TO airflow;
