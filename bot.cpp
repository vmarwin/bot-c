/* bot.cpp			*/
/* quote collector  */
/* version 1.08		*/

constexpr char version[] = { "1.08" };
constexpr int CSIZE = 80;
volatile bool is_thread_stop = false;

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <signal.h>
#include <string.h>
#include <sqlite3.h>
#include <vector>
#include <cgate.h> 
using namespace std;

struct SQuote
{
	double ask;
	double bid;
	uint64_t ms;

	SQuote(double& _ask, double& _bid, uint64_t& _ms) : 
		ask(_ask), bid(_bid), ms(_ms){};
};
struct SQData
{
	vector <SQuote> q;
};
struct SIsin
{
	char isin[CSIZE];
	int32_t isin_id;
};
struct SData
{
	vector <SIsin> d;
	vector <SQData> q;
	bool ref_is_ready{ false };
	bool hb_is_ready{ false };
	bool fut_is_ready{ false };
	cg_time_t server_time;
	char cserver_time[CSIZE];
};

void InterruptHandler(int signal)
{
	is_thread_stop = true;
}
CG_RESULT CallbackRef(cg_conn_t* conn, cg_listener_t* listener, struct cg_msg_t* msg, void* data)
{
	static size_t offset_isin = 0;
	static size_t offset_isin_id = 0;
	static size_t offset_replAct = 0; //i8

	SData* d = (SData*)data;

	switch (msg->type)
	{
	case CG_MSG_STREAM_DATA:
	{
		cg_msg_streamdata_t* replmsg = (cg_msg_streamdata_t*)msg;
		char* msg_data = (char*)replmsg->data;
		for (size_t i = 0; i < d->d.size(); i++)
		{
			if (strcmp(((char*)(msg_data + offset_isin)), d->d.at(i).isin) == 0 && *(int64_t*)(msg_data + offset_replAct) == 0)
			{
				d->d.at(i).isin_id = *(int32_t*)(msg_data + offset_isin_id);
			}
		}		
	}
	break;

	case CG_MSG_P2REPL_ONLINE:
	{
		printf("%s: Futures Reference online!\n", d->cserver_time);
		d->ref_is_ready = true;
	}
	break;

	case CG_MSG_CLOSE:
	{
		printf("%s: Futures Reference offline!\n", d->cserver_time);
		d->ref_is_ready = false;
	}
	break;

	case CG_MSG_OPEN:
	{
		struct cg_scheme_desc_t* schemedesc = 0;
		cg_lsn_getscheme(listener, &schemedesc);

		size_t msgidx = 0;
		for (cg_message_desc_t* msgdesc = schemedesc->messages; msgdesc; msgdesc = msgdesc->next, msgidx++)
		{
			size_t fieldindex = 0;
			if (strcmp(msgdesc->name, "fut_sess_contents") == 0)
			{
				for (cg_field_desc_t* fielddesc = msgdesc->fields; fielddesc; fielddesc = fielddesc->next, fieldindex++)
				{
					if (strcmp(fielddesc->name, "replAct") == 0 && strcmp(fielddesc->type, "i8") == 0)
					{
						offset_replAct = fielddesc->offset;
					}
					else if (strcmp(fielddesc->name, "isin_id") == 0 && strcmp(fielddesc->type, "i4") == 0)
					{
						offset_isin_id = fielddesc->offset;
					}
					else if (strcmp(fielddesc->name, "isin") == 0 && strcmp(fielddesc->type, "c25") == 0)
					{
						offset_isin = fielddesc->offset;
					}
				}
			}
		}
	}
	break;
	}
	return 0;
}
CG_RESULT CallbackHB(cg_conn_t* conn, cg_listener_t* listener, struct cg_msg_t* msg, void* data)
{
	static size_t offset_server_time{ 0 }; //t

	SData* d = (SData*)data;

	switch (msg->type)
	{
	case CG_MSG_STREAM_DATA:
	{
		cg_msg_streamdata_t* replmsg = (cg_msg_streamdata_t*)msg;
		char* msg_data = (char*)replmsg->data;

		d->server_time = *((cg_time_t*)(msg_data + offset_server_time));
		sprintf(d->cserver_time, "%04d.%02d.%02d %02d:%02d:%02d",
			d->server_time.year, d->server_time.month, d->server_time.day, d->server_time.hour,
			d->server_time.minute, d->server_time.second);
	}
	break;

	case CG_MSG_P2REPL_ONLINE:
	{
		printf("%s: Heartbeat online!\n", d->cserver_time);
		d->hb_is_ready = true;
	}
	break;

	case CG_MSG_CLOSE:
	{
		printf("%s: Heartbeat offline!\n", d->cserver_time);
		d->hb_is_ready = false;
	}
	break;

	case CG_MSG_OPEN:
	{
		struct cg_scheme_desc_t* schemedesc = 0;
		cg_lsn_getscheme(listener, &schemedesc);

		size_t msgidx = 0;
		for (cg_message_desc_t* msgdesc = schemedesc->messages; msgdesc; msgdesc = msgdesc->next, msgidx++)
		{
			size_t fieldindex = 0;
			if (strcmp(msgdesc->name, "heartbeat") == 0)
			{
				for (cg_field_desc_t* fielddesc = msgdesc->fields; fielddesc; fielddesc = fielddesc->next, fieldindex++)
				{
					if (strcmp(fielddesc->name, "server_time") == 0 && strcmp(fielddesc->type, "t") == 0)
					{
						offset_server_time = fielddesc->offset;
					}
				}
			}
		}
	}
	break;
	}

	return 0;
}
CG_RESULT CallbackQuote(cg_conn_t* conn, cg_listener_t* listener, struct cg_msg_t* msg, void* data)
{
	static size_t offset_replAct = 0; //i8
	static size_t offset_isin_id = 0;
	static size_t offset_best_buy = 0;
	static size_t offset_best_sell = 0;
	static size_t offset_mod_time_ns = 0; //u8

	SData* d = (SData*)data;

	switch (msg->type)
	{
	case CG_MSG_STREAM_DATA:
	{
		int64_t price_int = 0;
		int8_t price_scale = 0;
		double ask = 0.0;
		double bid = 0.0;
		uint64_t ms = 0;

		cg_msg_streamdata_t* replmsg = (cg_msg_streamdata_t*)msg;
		char* msg_data = (char*)replmsg->data;
		
		for (size_t i = 0; i < d->d.size(); i++)
		{
			if (*(int64_t*)(msg_data + offset_replAct) == 0 && d->d.at(i).isin_id == *((int32_t*)(msg_data + offset_isin_id)))
			{
				cg_bcd_get(((char*)(msg_data + offset_best_buy)), &price_int, &price_scale);
				bid = ((double)price_int) / (pow(10.0, price_scale));

				cg_bcd_get(((char*)(msg_data + offset_best_sell)), &price_int, &price_scale);
				ask = ((double)price_int) / (pow(10.0, price_scale));

				ms = *((uint64_t*)(msg_data + offset_mod_time_ns)) / 1000000;

				d->q.at(i).q.push_back(SQuote(ask, bid, ms));
			}
		}
		
	}
	break;
	case CG_MSG_OPEN:
	{
		struct cg_scheme_desc_t* schemedesc = 0;
		cg_lsn_getscheme(listener, &schemedesc);

		size_t msgidx = 0;
		for (cg_message_desc_t* msgdesc = schemedesc->messages; msgdesc; msgdesc = msgdesc->next, msgidx++)
		{
			size_t fieldindex = 0;
			if (strcmp(msgdesc->name, "common") == 0)
			{
				for (cg_field_desc_t* fielddesc = msgdesc->fields; fielddesc; fielddesc = fielddesc->next, fieldindex++)
				{
					if (strcmp(fielddesc->name, "replAct") == 0 && strcmp(fielddesc->type, "i8") == 0)
					{
						offset_replAct = fielddesc->offset;
					}
					else if (strcmp(fielddesc->name, "isin_id") == 0 && strcmp(fielddesc->type, "i4") == 0)
					{
						offset_isin_id = fielddesc->offset;
					}
					else if (strcmp(fielddesc->name, "best_buy") == 0 && strcmp(fielddesc->type, "d16.5") == 0)
					{
						offset_best_buy = fielddesc->offset;
					}
					else if (strcmp(fielddesc->name, "best_sell") == 0 && strcmp(fielddesc->type, "d16.5") == 0)
					{
						offset_best_sell = fielddesc->offset;
					}
					else if (strcmp(fielddesc->name, "mod_time_ns") == 0 && strcmp(fielddesc->type, "u8") == 0)
					{
						offset_mod_time_ns = fielddesc->offset;
					}					
				}
			}
		}
	}
	break;
	case CG_MSG_P2REPL_ONLINE:
	{
		printf("%s: Futures qoute online!\n", d->cserver_time);
		d->fut_is_ready = true;
	}
	break;
	case CG_MSG_CLOSE:
	{
		printf("%s: Futures qoute offline!\n", d->cserver_time);
		d->fut_is_ready = false;
	}
	break;
	}
	return 0;
};

int main(int argc, char* argv[])
{
	char key[] = { "" };
	char ref[CSIZE] = {};
	char fut[CSIZE] = {};
	int32_t close_bot = 86340;   // 23.59

	if (argc == 4)
	{
		close_bot = atoi(argv[1]);
		strcpy(ref, argv[2]);
		strcpy(fut, argv[3]);
	}
	else
	{
		printf("Bot version: %s; Use: %s close-time ref-key, fut-key\n", 
			version, argv[0]);
		return (0);
	}	

	printf("\nBot (%s) starting; Parameters: Close: %d:%02d\n\n",
		version, close_bot / 3600, (close_bot % 3600) / 60);
	
	SData d;
	d.d.resize(13);
	d.q.resize(13);

	strcpy(d.d.at(0).isin, "VTBR-3.22");
	strcpy(d.d.at(1).isin, "Si-3.22");
	strcpy(d.d.at(2).isin, "RTS-3.22");
	strcpy(d.d.at(3).isin, "BR-1.22");
	strcpy(d.d.at(4).isin, "GOLD-3.22");
	strcpy(d.d.at(5).isin, "SILV-3.22");
	strcpy(d.d.at(6).isin, "SBRF-3.22");
	strcpy(d.d.at(7).isin, "GAZR-3.22");
	strcpy(d.d.at(8).isin, "NG-1.22");
	strcpy(d.d.at(9).isin, "ED-3.22");
	strcpy(d.d.at(10).isin, "SPYF-3.22");
	strcpy(d.d.at(11).isin, "Eu-3.22");
	strcpy(d.d.at(12).isin, "MXI-3.22");

	cg_listener_t* listener_ref = 0;
	cg_listener_t* listener_hb = 0;
	cg_listener_t* listener_fut = 0;

	cg_conn_t* conn_ref;
	cg_conn_t* conn_fut;

	uint32_t state = 0;
	int32_t result = 0;
	char c[200];
	int32_t current_sec = 0;

	signal(SIGINT, InterruptHandler);
	sprintf(c, "ini=bot.ini;minloglevel=info;subsystems=mq,replclient;key=%s", key);
	result = cg_env_open(c);

	if (result != CG_ERR_OK)
	{
		return(-1);
	}

	sprintf(c, "p2lrpcq://127.0.0.1:4001;app_name=%s;local_pass=1234;timeout=5000;local_timeout=500;lrpcq_buf=0;name=ref", ref);
	result = cg_conn_new(c, &conn_ref);
	result = cg_lsn_new(conn_ref, "p2repl://FORTS_REFDATA_REPL;tables=fut_sess_contents;name=ref_info", &CallbackRef, &d, &listener_ref);
	result = cg_lsn_new(conn_ref, "p2repl://FORTS_TRADE_REPL;tables=heartbeat;name=hb_info", &CallbackHB, &d, &listener_hb);

	sprintf(c, "p2lrpcq://127.0.0.1:4001;app_name=%s;local_pass=1234;timeout=5000;local_timeout=500;lrpcq_buf=0;name=futures_quote", fut);
	result = cg_conn_new(c, &conn_fut);
	result = cg_lsn_new(conn_fut, "p2repl://FORTS_COMMON_REPL;tables=common;name=futures_quote_info", &CallbackQuote, &d, &listener_fut);

	// main process
	while (!is_thread_stop)
	{
		// refer process
		result = cg_conn_getstate(conn_ref, &state);
		switch (state)
		{
		case CG_STATE_CLOSED:
			result = cg_conn_open(conn_ref, 0);
			break;
		case CG_STATE_ACTIVE:
			result = cg_conn_process(conn_ref, 0, 0);

			// heartbeat ref
			result = cg_lsn_getstate(listener_hb, &state);
			switch (state)
			{
			case CG_STATE_CLOSED:
				result = cg_lsn_open(listener_hb, 0);
				break;
			case CG_STATE_ERROR:
				result = cg_lsn_close(listener_hb);
				break;
			}

			// futures ref
			if (d.hb_is_ready)
			{
				result = cg_lsn_getstate(listener_ref, &state);
				switch (state)
				{
				case CG_STATE_CLOSED:
					result = cg_lsn_open(listener_ref, 0);
					break;
				case CG_STATE_ERROR:
					result = cg_lsn_close(listener_ref);
					break;
				}
			}

			break;
		case CG_STATE_ERROR:
			result = cg_conn_close(conn_ref);
			break;
		}

		// fut quote process
		if (d.ref_is_ready)
		{
			result = cg_conn_getstate(conn_fut, &state);
			switch (state)
			{
			case CG_STATE_CLOSED:
				result = cg_conn_open(conn_fut, 0);
				break;
			case CG_STATE_ACTIVE:
				result = cg_conn_process(conn_fut, 0, 0);
				result = cg_lsn_getstate(listener_fut, &state);
				switch (state)
				{
				case CG_STATE_CLOSED:
					result = cg_lsn_open(listener_fut, 0);
					break;
				case CG_STATE_ERROR:
					result = cg_lsn_close(listener_fut);
					break;
				}
				break;
			case CG_STATE_ERROR:
				result = cg_conn_close(conn_fut);
				break;
			}
		}

		if (d.hb_is_ready)
		{
			current_sec = (d.server_time.hour * 3600 + d.server_time.minute * 60 + d.server_time.second);
			if (current_sec > close_bot)
			{
				is_thread_stop = true;
			}
		}
	}

	// db backup process
	for (size_t i = 0; i < d.q.size(); i++)
	{
		sqlite3* dbh(0);
		char* err(0);
		char sql[600];
		sprintf(c, "%s-%02d-%02d-%04d.db", d.d.at(i).isin, d.server_time.day, d.server_time.month, d.server_time.year);
		sqlite3_open(c, &dbh);

		sqlite3_exec(dbh, "Create table if not exists TQuote (id INTEGER, ask REAL, bid REAL, ms INTEGER, PRIMARY KEY(id))", 0, 0, &err);
		sqlite3_exec(dbh, "BEGIN TRANSACTION;", 0, 0, &err);

		for (size_t j = 0; j < d.q.at(i).q.size(); j++)
		{
			sprintf(sql, "INSERT INTO TQuote (ask, bid, ms) VALUES (%G, %G, %lu)",
				d.q.at(i).q[j].ask, d.q.at(i).q[j].bid, d.q.at(i).q[j].ms);
			sqlite3_exec(dbh, sql, 0, 0, &err);
		}

		sqlite3_exec(dbh, "END TRANSACTION;", 0, 0, &err);
		sqlite3_close(dbh);
	}
	
	// out process
	result = cg_lsn_close(listener_ref);
	result = cg_lsn_destroy(listener_ref);
	result = cg_lsn_close(listener_hb);
	result = cg_lsn_destroy(listener_hb);
	result = cg_lsn_close(listener_fut);
	result = cg_lsn_destroy(listener_fut);

	result = cg_conn_close(conn_fut);
	result = cg_conn_destroy(conn_fut);
	result = cg_conn_close(conn_ref);
	result = cg_conn_destroy(conn_ref);

	cg_env_close();

	printf("%s: Bot shutdown\n", d.cserver_time);

	return(0);
};
