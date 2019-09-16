package com.cloudera.solr;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class GenerateSolrTestData {
	public static long getId() {

		return (long) (Math.random() * 1000000000000l);

	}

	public static String getRadomCOLLECTIONDATE() {
		String year[] = { "2018" };
		String month[] = { "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12" };
		String day[] = { "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16",
				"17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28" };
		String hour[] = { "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14",
				"15", "16", "17", "18", "19", "20", "21", "22", "23" };
		String minute[] = { "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14",
				"15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31",
				"32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48",
				"49", "50", "51", "52", "53", "54", "55", "56", "57", "58", "59" };
		String second[] = { "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14",
				"15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31",
				"32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48",
				"49", "50", "51", "52", "53", "54", "55", "56", "57", "58", "59" };
		int index1 = (int) (Math.random() * year.length);
		int index2 = (int) (Math.random() * month.length);
		int index3 = (int) (Math.random() * day.length);
		int index4 = (int) (Math.random() * hour.length);
		int index5 = (int) (Math.random() * minute.length);
		int index6 = (int) (Math.random() * second.length);
		String coliectiondate = year[index1] + "-" + month[index2] + "-" + day[index3] + "T" + hour[index4] + ":"
				+ minute[index5] + ":" + second[index6] + "Z";
		return coliectiondate;

	}

	public static String getRandomText() {
		String test[] = { "accumulo-core-1.6.0.jar", "accumulo-fate-1.6.0.jar", "accumulo-start-1.6.0.jar",
				"accumulo-trace-1.6.0.jar", "activation-1.1.jar", "activemq-client-5.10.2.jar",
				"akka-actor_2.10-2.2.3-shaded-protobuf.jar", "akka-remote_2.10-2.2.3-shaded-protobuf.jar",
				"akka-slf4j_2.10-2.2.3-shaded-protobuf.jar", "akuma-1.9.jar", "algebird-core_2.10-0.6.0.jar",
				"annotations-api.jar", "ant-1.5.jar", "ant-1.6.5.jar", "ant-1.8.1.jar", "ant-1.9.1.jar",
				"ant-contrib-1.0b3.jar", "ant-eclipse-1.0-jvm1.2.jar", "ant-launcher-1.8.1.jar",
				"ant-launcher-1.9.1.jar", "antlr-2.7.7.jar", "antlr-3.4.jar", "antlr-runtime-3.3.jar",
				"antlr-runtime-3.4.jar", "antlr-runtime-3.5.jar", "aopalliance-1.0.jar", "apacheds-i18n-2.0.0-M15.jar",
				"apacheds-kerberos-codec-2.0.0-M15.jar", "apache-log4j-extras-1.1.jar",
				"apache-log4j-extras-1.2.17.jar", "apache-mime4j-core-0.7.2.jar", "apache-mime4j-dom-0.7.2.jar",
				"api-asn1-api-1.0.0-M20.jar", "api-asn1-ber-1.0.0-M20.jar", "api-i18n-1.0.0-M20.jar",
				"api-ldap-model-1.0.0-M20.jar", "api-util-1.0.0-M20.jar", "argparse4j-0.4.3.jar",
				"arpack_combined_all-0.1.jar", "asm-3.1.jar", "asm-3.2.jar", "asm-4.1.jar","xz-1.4.jar", "zkclient-0.3.jar", "zookeeper-3.4.5-cdh5.6.0.jar" };
		int index1 = (int) (Math.random() * test.length);
		return test[index1];
	}
	public static String getRandomTextCh() {
		String test[] = {
				"贾玲，原名贾裕玲。1982年4月29日出生于湖北襄阳，毕业于中央戏剧学院。喜剧女演员，师从冯巩，发起并创立酷口相声。2003年获《全国相声小品邀请赛》相声一等奖。2006年《中央电视台》第三届相声大赛专业组二等奖。2009年7月，由贾玲、邹僧等人创办的新笑声客栈开张，成为酷口相声的大本营。2010年2月14日，贾玲首次登上央视春晚的舞台表演相声《大话捧逗》，并获“我最喜爱的春晚节目”曲艺组三等奖。2011年2月2日，再次登上央视春晚舞台，表演相声《芝麻开门》。",
				"要实现近实时搜索，就必须有一种机制来实时的处理数据然后生成到solr的索引中去，flume-ng刚好提供了这样一种机>制，它可以实时收集数据，然后通过MorphlineSolrSink对数据进行ETL，最后写入到solr的索引中，这样就能在solr搜索引擎中近实时的查询到新进来的数据了由贾玲人。",
				"现实生活中我们都知道大多数网站或应用都必须具有某种搜索功能，问题是搜索功能往往是巨大的资源消耗并且它们由于沉重的数据库加载而拖垮你的应用的性能。",
				"实际还是围绕着Agent的三个组件Source、Channel、Sink来看它能够支持哪些技术或协议。我们不再对各个组件支持的协议详细配置进行说明，通过列表的方式分别对三个组件进行概要说明",
				"下面写一个最简单的Hello World例子，以便对RESTful WebService有个感性认识。因为非常专业理论化的描述RESTful WebService是一件理解起来很痛苦的事情。看看例子就知道个大概了，再看理论就容易理解多了。",
				"据香港经济日报报道，传小米可能在下周向港交所提交上市申请。经济日报此前还报道，小米最近数月不乏上市前股东售股活动，售股价格显示公司估值介乎650亿至700亿美元。此前，曾有多个小米估值的版本出现，比如1000亿美元，甚至2000亿美元，小米方面都未进行置评",
				"最近，中超新晋土豪苏宁可谓是频出大手笔。夏窗尚未开启，苏宁就早早开始谋划了。尽管距离泰达与恒大的比赛还有2天的时间，但比赛的硝烟已经开始弥漫。",
				"据美国媒体报道，美国当地时间21日上午，流行音乐传奇人物王子(Prince)被发现死于位于明尼苏达的住所内，医务人员进行了紧急抢救，但最终回天无力，享年57岁。",
				"Solr可以和Hadoop一起使用。由于Hadoop处理大量数据，Solr帮助我们从这么大的源中找到所需的信息。不仅限于搜索，Solr也可以用于存储目的。像其他NoSQL数据库一样，它是一种非关系数据存储和处理技术。",
				"为了在CNET网络的公司网站上添加搜索功能，Yonik Seely于2004年创建了Solr。并在2006年1月，它成为Apache软件基金会下的一个开源项目。并于2016年发布最新版本Solr 6.0，支持并行SQL查询的执行。",
				"数据库本身不能实现分词效果，而只能使用模糊查询，但是模糊查询非常低效，查询速度比较慢，由于在实际生活中，一般搜索是用的比较多的，这样数据库压力自然就很大，所以我们就让供专业的solr来做搜索功能",
				"自7月18日，陆慷在外交部记者会上宣布将不再担任外交部新闻司司长、发言人。谁将出任外交部新闻司司长？这一问题备受大家关注。此前已有外媒猜测，华春莹将成为陆慷的继任者。",
				"她在发言人“首秀”中说：“我将竭尽全力，帮助大家及时、准确、全面地了解中国的外交政策，我也期待着与大家进行真诚的沟通、平等的交流、良好的合作，为增进中国与世界的相互了解和理解、信任和合作做出积极努力。”"};
		int index1 = (int) (Math.random() * test.length);
		return test[index1];
	}
	public static String getRandomTextEn() {
		String test[] = {
				"One The metastore database is an important aspect of the Hive infrastructure. It is a separate database， relying on a traditional RDBMS such as MySQL or PostgreSQL， that holds metadata about Hive databases， tables， columns， partitions， and Hadoop-specific information such as the underlying data files and HDFS block locations.The metastore database is shared by other components. For example， the same tables can be inserted into， queried， altered， and so on by both Hive and Impala. Although you might see references to the Hive metastore， be aware that the metastore database is used broadly across the Hadoop ecosystem， even in cases where you are not using Hive itself.The metastore database is relatively compact， with fast-changing data. Backup， replication， and other kinds of management operations affect this database. See Configuring the Hive Metastore for CDH for details about configuring the Hive metastore.Cloudera recommends that you deploy the Hive metastore， which stores the metadata for Hive tables and partitions， in remote mode. In this mode the metastore service runs in its own JVM process and other services， such as HiveServer2， HCatalog， and Apache Impala communicate with the metastore using the Thrift network API.",
				"Two HiveServer2 is a server interface that enables remote clients to submit queries to Hive and retrieve the results. HiveServer2 supports multi-client concurrency， capacity planning controls， Sentry authorization， Kerberos authentication， LDAP， SSL， and provides support for JDBC and ODBC clients.HiveServer2 is a container for the Hive execution engine. For each client connection， it creates a new execution context that serves Hive SQL requests from the client. It supports JDBC clients， such as the Beeline CLI， and ODBC clients. Clients connect to HiveServer2 through the Thrift API-based Hive service.See Configuring HiveServer2 for CDH for details on configuring HiveServer2 and see Starting， Stopping， and Using HiveServer2 in CDH for details on starting/stopping the HiveServer2 service and information about using the Beeline CLI to connect to HiveServer2. For details about managing HiveServer2 with its native web user interface (UI)， see Using HiveServer2 Web UI in CDH.Hive traditionally uses MapReduce behind the scenes to parallelize the work， and perform the low-level steps of processing a SQL statement such as sorting and filtering. Hive can also use Spark as the underlying computation and parallelization engine. See Running Apache Hive on Spark in CDH for details about configuring Hive to use Spark as its execution engine and see Tuning Apache Hive on Spark in CDH for details about tuning Hive on Spark.",
				"Three Clients - Entities including Hue， ODBC clients， JDBC clients， and the Impala Shell can all interact with Impala. These interfaces are typically used to issue queries or complete administrative tasks such as connecting to Impala.Hive Metastore - Stores information about the data available to Impala. For example， the metastore lets Impala know what databases are available and what the structure of those databases is. As you create， drop， and alter schema objects， load data into tables， and so on through Impala SQL statements， the relevant metadata changes are automatically broadcast to all Impala nodes by the dedicated catalog service introduced in Impala 1.2.Impala - This process， which runs on DataNodes， coordinates and executes queries. Each instance of Impala can receive， plan， and coordinate queries from Impala clients. Queries are distributed among Impala nodes， and these nodes then act as workers， executing parallel query fragments.HBase and HDFS - Storage for data to be queried.User applications send SQL queries to Impala through ODBC or JDBC， which provide standardized querying interfaces. The user application may connect to any impalad in the cluster. This impalad becomes the coordinator for the query.Impala parses the query and analyzes it to determine what tasks need to be performed by impalad instances across the cluster. Execution is planned for optimal efficiency.",
				"Four Apache Spark is a general framework for distributed computing that offers high performance for both batch and interactive processing. It exposes APIs for Java， Python， and Scala and consists of Spark core and several related projects.You can run Spark applications locally or distributed across a cluster， either by using an interactive shell or by submitting an application. Running Spark applications interactively is commonly performed during the data-exploration phase and for ad hoc analysis.To run applications distributed across a cluster， Spark requires a cluster manager. In CDH 6， Cloudera supports only the YARN cluster manager. When run on YARN， Spark application processes are managed by the YARN ResourceManager and NodeManager roles. Spark Standalone is no longer supported.For detailed API information， see the Apache Spark project site.Note: Although this document makes some references to the external Spark site， not all the features， components， recommendations， and so on are applicable to Spark when used on CDH. Always cross-check the Cloudera documentation before building a reliance on some aspect of Spark that might not be supported or recommended by Cloudera. In particular， see Apache Spark Known Issues for components and features to avoid.",
				"Five The following Spark features are not supported:Apache Spark experimental features/APIs are not supported unless stated otherwise.Using the JDBC Datasource API to access Hive or Impala is not supportedADLS not Supported for All Spark Components. Microsoft Azure Data Lake Store (ADLS) is a cloud-based filesystem that you can access through Spark applications. Spark with Kudu is not currently supported for ADLS data. (Hive on Spark is available for ADLS in CDH 5.12 and higher.)IPython / Jupyter notebooks is not supported. The IPython notebook system (renamed to Jupyter as of IPython 4.0) is not supported.Certain Spark Streaming features not supported. The mapWithState method is unsupported because it is a nascent unstable API.Thrift JDBC/ODBC server is not supportedSpark SQL CLI is not supportedGraphX is not supportedSparkR is not supportedStructured Streaming is supported， but the following features of it are not:Continuous processing， which is still experimental， is not supported.Stream static joins with HBase have not been tested and therefore are not supported.Spark cost-based optimizer (CBO) not supported.Consult Apache Spark Known Issues for a comprehensive list of Spark 2 features that are not supported with CDH 6.",
				"Six Cloudera Manager models CDH and managed services: their roles， configurations， and inter-dependencies. Model state captures what is supposed to run where， and with what configurations. For example， model state captures the fact that a cluster contains 17 hosts， each of which is supposed to run a DataNode. You interact with the model through the Cloudera Manager Admin Console configuration screens and API and operations such as Add Service.Runtime state is what processes are running where， and what commands (for example， rebalance HDFS or run a Backup/Disaster Recovery schedule or rolling restart or stop) are currently running. The runtime state includes the exact configuration files needed to run a process. When you select Start in the Cloudera Manager Admin Console， the server gathers up all the configuration for the relevant services and roles， validates it， generates the configuration files， and stores them in the database.When you update a configuration (for example， the Hue Server web port)， you have updated the model state. However， if Hue is running while you do this， it is still using the old port. When this kind of mismatch occurs， the role is marked as having an outdated configuration. To resynchronize， you restart the role (which triggers the configuration re-generation and process restart).While Cloudera Manager models all of the reasonable configurations， some cases inevitably require special handling. To allow you to workaround， for example， a bug or to explore unsupported options， Cloudera Manager supports an advanced configuration snippet mechanism that lets you add properties directly to the configuration files.",
				"Seven You can set configuration at the service instance (for example， HDFS) or role instance (for example， the DataNode on host17). An individual role inherits the configurations set at the service level. Configurations made at the role level override those inherited from the service level. While this approach offers flexibility， configuring a set of role instances in the same way can be tedious.Cloudera Manager supports role groups， a mechanism for assigning configurations to a group of role instances. The members of those groups then inherit those configurations. For example， in a cluster with heterogeneous hardware， a DataNode role group can be created for each host type and the DataNodes running on those hosts can be assigned to their corresponding role group. That makes it possible to set the configuration for all the DataNodes running on the same hardware by modifying the configuration of one role group. The HDFS service discussed earlier has the following role groups defined for the service's rolesIn addition to making it easy to manage the configuration of subsets of roles， role groups also make it possible to maintain different configurations for experimentation or managing shared clusters for different users or workloads.",
				"Eight Cloudera Manager can deploy client configurations within the cluster; each applicable service has a Deploy Client Configuration action. This action does not necessarily deploy the client configuration to the entire cluster; it only deploys the client configuration to all the hosts that this service has been assigned to. For example， suppose a cluster has 10 hosts， and a MapReduce service is running on hosts 1-9. When you use Cloudera Manager to deploy the MapReduce client configuration， host 10 will not get a client configuration， because the MapReduce service has no role assigned to it. This design is intentional to avoid deploying conflicting client configurations from multiple services.To deploy a client configuration to a host that does not have a role assigned to it you use a gateway. A gateway is a marker to convey that a service should be accessible from a particular host. Unlike all other roles it has no associated process. In the preceding example， to deploy the MapReduce client configuration to host 10， you assign a MapReduce gateway role to that host.Gateways can also be used to customize client configurations for some hosts. Gateways can be placed in role groups and those groups can be configured differently. However， unlike role instances， there is no way to override configurations for gateway instances.",
				"Nine Cloudera Manager provides several features to manage the hosts in your Hadoop clusters. The first time you run Cloudera Manager Admin Console you can search for hosts to add to the cluster and once the hosts are selected you can map the assignment of CDH roles to hosts. Cloudera Manager automatically deploys all software required to participate as a managed host in a cluster: JDK， Cloudera Manager Agent， CDH， Impala， Solr， and so on to the hosts.Once the services are deployed and running， the Hosts area within the Admin Console shows the overall status of the managed hosts in your cluster. The information provided includes the version of CDH running on the host， the cluster to which the host belongs， and the number of roles running on the host. Cloudera Manager provides operations to manage the lifecycle of the participating hosts and to add and delete hosts. The Cloudera Management Service Host Monitor role performs health tests and collects host metrics to allow you to monitor the health and performance of the hosts.Resource management helps ensure predictable behavior by defining the impact of different services on cluster resources. Use resource management to",
				"Ten Authentication is a process that requires users and services to prove their identity when trying to access a system resource. Organizations typically manage user identity and authentication through various time-tested technologies， including Lightweight Directory Access Protocol (LDAP) for identity， directory， and other services， such as group management， and Kerberos for authentication.Cloudera clusters support integration with both of these technologies. For example， organizations with existing LDAP directory services such as Active Directory (included in Microsoft Windows Server as part of its suite of Active Directory Services) can leverage the organization's existing user accounts and group listings instead of creating new accounts throughout the cluster. Using an external system such as Active Directory or OpenLDAP is required to support the user role authorization mechanism implemented in Cloudera Navigator.For authentication， Cloudera supports integration with MIT Kerberos and with Active Directory. Microsoft Active Directory supports Kerberos for authentication in addition to its identity management and directory functionality， that is， LDAP.These systems are not mutually exclusive. For example， Microsoft Active Directory is an LDAP directory service that also provides Kerberos authentication services， and Kerberos credentials can be stored and managed in an LDAP directory service. Cloudera Manager Server， CDH nodes， and Cloudera Enterprise components， such as Cloudera Navigator， Apache Hive， Hue， and Impala， can all make use of Kerberos authentication."};

		int index1 = (int) (Math.random() * test.length);
		return test[index1];

	}

	public static int getInt(){
		int i = 0;
		i++;
		return 1;
	}

	public static String getData(int i) {
		StringBuffer sbf = new StringBuffer();
		sbf.append(i + "," + getDouble() + "," + getId() + "," + getDouble() + ","
				+ getId() + "," + getRandomText() + "," + getRandomTextEn() + ","
				+ getRandomTextCh() + "," + getRadomCOLLECTIONDATE() + "," + getRadomCOLLECTIONDATE());
		return sbf.toString();
	}

	public static double getDouble(){
		return (double) Math.random() * 1000;
	}

	public static void write(int n, String file) {
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(file, true), 4194304);
			bw.write("number,firstDouble,firstNo,secondDouble,secondNo,jarName,enText,cnText,firstTime,secondTime" + "\r\n");
			for (int i = 0; i < n; i++) {
				bw.write(getData(i) + "\r\n");
			}
			System.out.println("数据生成完毕！" + file);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("数据生成异常！");
		} finally {
			try {
				bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		write(600000, "/root/generate_data/csv/data.csv");
	}
}