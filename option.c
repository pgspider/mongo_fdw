/*-------------------------------------------------------------------------
 *
 * option.c
 * 		FDW option handling for mongo_fdw
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2022, EnterpriseDB Corporation.
 * Portions Copyright (c) 2012–2014 Citus Data, Inc.
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 * 		option.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#if PG_VERSION_NUM >= 160000
#include "utils/varlena.h"
#endif
#include "mongo_wrapper.h"

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses postgres_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
extern Datum mongo_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(mongo_fdw_validator);

/*
 * mongo_fdw_validator
 *		Validates options given to one of the following commands:
 *		foreign data wrapper, server, user mapping, or foreign table.
 *
 * This function errors out if the given option name or its value is considered
 * invalid.
 */
Datum
mongo_fdw_validator(PG_FUNCTION_ARGS)
{
	Datum		optionArray = PG_GETARG_DATUM(0);
	Oid			optionContextId = PG_GETARG_OID(1);
	List	   *optionList = untransformRelOptions(optionArray);
	ListCell   *optionCell;

	foreach(optionCell, optionList)
	{
		DefElem    *optionDef = (DefElem *) lfirst(optionCell);
		char	   *optionName = optionDef->defname;
		bool		optionValid = false;
		int32		optionIndex;

		for (optionIndex = 0; optionIndex < ValidOptionCount; optionIndex++)
		{
			const MongoValidOption *validOption;

			validOption = &(ValidOptionArray[optionIndex]);

			if ((optionContextId == validOption->optionContextId) &&
				(strncmp(optionName, validOption->optionName, NAMEDATALEN) == 0))
			{
				optionValid = true;
				break;
			}
		}

		/* If invalid option, display an informative error message */
		if (!optionValid)
		{
#if PG_VERSION_NUM >= 160000
			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with a valid option that looks similar, if there is one.
			 */
			const char		   *closest_match;
			ClosestMatchState	match_state;
			bool				has_valid_options = false;

			initClosestMatch(&match_state, optionName, 4);
			for (optionIndex = 0; optionIndex < ValidOptionCount; optionIndex++)
			{
				const MongoValidOption *validOption;

				validOption = &(ValidOptionArray[optionIndex]);

				if (optionContextId == validOption->optionContextId)
				{
					has_valid_options = true;
					updateClosestMatch(&match_state, validOption->optionName);
				}
			}
			closest_match = getClosestMatch(&match_state);
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", optionName),
					 has_valid_options ? closest_match ?
					 errhint("Perhaps you meant the option \"%s\".",
							 closest_match) : 0 :
					 errhint("There are no valid options in this context.")));
#else
			StringInfo			optionNamesString;
			optionNamesString = mongo_option_names_string(optionContextId);
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", optionName),
					 errhint("Valid options in this context are: %s.",
							 optionNamesString->data)));
#endif
		}

		/* If port option is given, error out if its value isn't an integer */
		if (strncmp(optionName, OPTION_NAME_PORT, NAMEDATALEN) == 0)
		{
			long 		port;
			char	   *intString = defGetString(optionDef);
			char	   *endp;

			port = strtol(intString, &endp, 10);
			if (port < 0 || port > USHRT_MAX)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("port value \"%s\" is out of range for type %s",
								intString, "unsigned short")));
		}
		else if (strcmp(optionName, OPTION_NAME_USE_REMOTE_ESTIMATE) == 0
				 || strcmp(optionName, OPTION_NAME_ENABLE_JOIN_PUSHDOWN) == 0
#ifdef META_DRIVER
				 || strcmp(optionName, OPTION_NAME_WEAK_CERT) == 0 ||
				 strcmp(optionName, OPTION_NAME_SSL) == 0
#endif
				 )
		{
			/* These accept only boolean values */
			(void) defGetBoolean(optionDef);
		}
	}

	PG_RETURN_VOID();
}

/*
 * mongo_option_names_string
 *		Finds all options that are valid for the current context, and
 *		concatenates these option names in a comma separated string.
 */
StringInfo
mongo_option_names_string(Oid currentContextId)
{
	StringInfo	optionNamesString = makeStringInfo();
	bool		firstOptionPrinted = false;
	int32		optionIndex;

	for (optionIndex = 0; optionIndex < ValidOptionCount; optionIndex++)
	{
		const MongoValidOption *validOption;

		validOption = &(ValidOptionArray[optionIndex]);

		/* If option belongs to current context, append option name */
		if (currentContextId == validOption->optionContextId)
		{
			if (firstOptionPrinted)
				appendStringInfoString(optionNamesString, ", ");

			appendStringInfoString(optionNamesString, validOption->optionName);
			firstOptionPrinted = true;
		}
	}

	return optionNamesString;
}

/*
 * mongo_get_options
 *		Returns the option values to be used when connecting to and querying
 *		MongoDB.
 *
 * To resolve these values, the function checks the foreign table's options,
 * and if not present, falls back to default values.
 */
MongoFdwOptions *
mongo_get_options(Oid foreignTableId, Oid userid)
{
	ForeignTable *foreignTable;
	ForeignServer *foreignServer;
	UserMapping *mapping;
	List	   *optionList = NIL;
	MongoFdwOptions *options;
	ListCell   *lc;

	foreignTable = GetForeignTable(foreignTableId);
	foreignServer = GetForeignServer(foreignTable->serverid);
	mapping = GetUserMapping(userid, foreignTable->serverid);

	optionList = mongo_list_concat(optionList, foreignServer->options);
	optionList = mongo_list_concat(optionList, foreignTable->options);
	optionList = mongo_list_concat(optionList, mapping->options);

	options = (MongoFdwOptions *) palloc0(sizeof(MongoFdwOptions));

	options->use_remote_estimate = false;
	options->enable_join_pushdown = true;
#ifdef META_DRIVER
	options->ssl = false;
	options->weak_cert_validation = false;
#endif

	/* Loop through the options */
	foreach(lc, optionList)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

#ifdef META_DRIVER
		if (strcmp(def->defname, OPTION_NAME_READ_PREFERENCE) == 0)
			options->readPreference = defGetString(def);

		else if (strcmp(def->defname, OPTION_NAME_AUTHENTICATION_DATABASE) == 0)
			options->authenticationDatabase = defGetString(def);

		else if (strcmp(def->defname, OPTION_NAME_REPLICA_SET) == 0)
			options->replicaSet = defGetString(def);

		else if (strcmp(def->defname, OPTION_NAME_SSL) == 0)
			options->ssl = defGetBoolean(def);

		else if (strcmp(def->defname, OPTION_NAME_PEM_FILE) == 0)
			options->pem_file = defGetString(def);

		else if (strcmp(def->defname, OPTION_NAME_PEM_PWD) == 0)
			options->pem_pwd = defGetString(def);

		else if (strcmp(def->defname, OPTION_NAME_CA_FILE) == 0)
			options->ca_file = defGetString(def);

		else if (strcmp(def->defname, OPTION_NAME_CA_DIR) == 0)
			options->ca_dir = defGetString(def);

		else if (strcmp(def->defname, OPTION_NAME_CRL_FILE) == 0)
			options->crl_file = defGetString(def);

		else if (strcmp(def->defname, OPTION_NAME_WEAK_CERT) == 0)
			options->weak_cert_validation = defGetBoolean(def);

		else /* This is for continuation */
#endif

		if (strcmp(def->defname, OPTION_NAME_ADDRESS) == 0)
			options->svr_address = defGetString(def);

		else if (strcmp(def->defname, OPTION_NAME_PORT) == 0)
			options->svr_port = atoi(defGetString(def));

		else if (strcmp(def->defname, OPTION_NAME_DATABASE) == 0)
			options->svr_database = defGetString(def);

		else if (strcmp(def->defname, OPTION_NAME_COLLECTION) == 0)
			options->collectionName = defGetString(def);

		else if (strcmp(def->defname, OPTION_NAME_COLUMN_NAME) == 0)
			options->column_name = defGetString(def);

		else if (strcmp(def->defname, OPTION_NAME_USERNAME) == 0)
			options->svr_username = defGetString(def);

		else if (strcmp(def->defname, OPTION_NAME_PASSWORD) == 0)
			options->svr_password = defGetString(def);

		else if (strcmp(def->defname, OPTION_NAME_USE_REMOTE_ESTIMATE) == 0)
			options->use_remote_estimate = defGetBoolean(def);

		else if (strcmp(def->defname, OPTION_NAME_ENABLE_JOIN_PUSHDOWN) == 0)
			options->enable_join_pushdown = defGetBoolean(def);
	}

	/* Default values, if required */
	if (!options->svr_address)
		options->svr_address = DEFAULT_IP_ADDRESS;

	if (!options->svr_port)
		options->svr_port = DEFAULT_PORT_NUMBER;

	if (!options->svr_database)
		options->svr_database = DEFAULT_DATABASE_NAME;

	if (!options->collectionName)
		options->collectionName= get_rel_name(foreignTableId);

	return options;
}

void
mongo_free_options(MongoFdwOptions *options)
{
	if (options)
		pfree(options);
}
