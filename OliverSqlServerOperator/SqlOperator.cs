using System.Data;
using System.Data.SqlClient;



namespace OliverSqlServerOperator
{
    /// <summary>
    /// sqlserver数据库交互类。使用前应先修改连接字符串connectionStr。
    /// 
    /// 数据库用户登录使用："Data Source=.\\SQLEXPRESS;Initial Catalog=FaultTreeAnaysis;Persist Security Info=True;User ID=sa;Password=root" 将其中Data Source设置为服务器名（本机用“.”表示,EXPRESS版本后加“\\SQLEXPRESS”），Initial Catalog设置为数据库名。User ID 为用户名，Password为密码。
    /// 
    /// Windows身份验证直接使用 server=DESKTOP-VVH84UI;database=TestDB;Trusted_Connection=SSPI 其中server为服务器名，database为数据库名。
    /// </summary>
    public class SqlOperator
    {
        #region Variable Declarations
        static string connectionStr = "";//连接字符串
        static SqlConnection con = new SqlConnection();
        #endregion

        #region PublicFunc
        public SqlOperator(string connectionString = "", string serverName = "", string dataBaseName = "", string username = "sa", string password = "", bool isSSPI = false)
        {
            if (connectionString == "")
            {
                SetConnection(serverName, dataBaseName, username, password, isSSPI);
            }
            else
            {
                connectionStr = connectionString;
            }
        }

        /// <summary>
        /// 设置数据库连接字符串
        /// </summary>
        /// <param name="connectionString">连接字符串</param>
        public static void SetConnectionString(string connectionString)
        {
            connectionStr = connectionString;
        }

        /// <summary>
        /// 设置数据库连接字符串
        /// </summary>
        /// <param name="serverName"></param>
        /// <param name="dataBaseName"></param>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <param name="isSSPI"></param>
        public static void SetConnection(string serverName, string dataBaseName, string username = "sa", string password = "", bool isSSPI = false)
        {
            if (isSSPI)
            {
                connectionStr = $"server={serverName};database={dataBaseName};Trusted_Connection=SSPI";
            }
            else
            {
                connectionStr = $"Data Source={serverName};Initial Catalog={dataBaseName};Persist Security Info=True;User ID={username};Password={password}";
            }
        }

        /// <summary>
        /// 以SqlDataReader的形式返回被请求的数据，获取数据调用SqlDataReader[0]
        /// </summary>
        /// <param name="str">需要执行的命令</param>
        /// <returns>注意，返回SqlDataReader连接并未关闭，可以继续对数据库进行操作。需调用Close关闭连接</returns>
        public static SqlDataReader ExecuteSQLReturnDR(string str)
        {
            ConnectToSQL();
            SqlCommand com = new SqlCommand();
            com.Connection = con;
            com.CommandType = CommandType.Text;
            com.CommandText = str;

            //不需要关闭连接 因为SqlDataReader还要对数据库进行操作
            //DisconnectSQL();
            return com.ExecuteReader();
        }

        /// <summary>
        /// 在数据库中执行命令，并以datatable的形式返回结果
        /// </summary>
        /// <param name="str">需要执行的命令</param>
        /// <returns></returns>
        public static DataTable ExecuteSQLReturnDT(string str)
        {
            ConnectToSQL();
            SqlDataAdapter myDataAdapter = new SqlDataAdapter(str, con);
            DataSet myDataSet = new DataSet();
            myDataAdapter.Fill(myDataSet, "table");
            DisconnectSQL();
            return myDataSet.Tables["table"];

        }

        /// <summary>
        /// 执行命令，返回结果table中第一个cell内容
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        public static string ExecuteSQLGetFirstCell(string str)
        {
            return ExecuteSQLReturnDT(str).Rows[0][0].ToString();
        }

        /// <summary>
        /// 在数据库中执行命令
        /// </summary>
        /// <param name="str">需要执行的命令</param>
        public static void ExecuteSQL(string str)
        {
            ConnectToSQL();
            SqlCommand com = new SqlCommand();
            com.Connection = con;
            com.CommandType = CommandType.Text;
            com.CommandText = str;
            SqlDataReader dr = com.ExecuteReader();
            dr.Close();
        }

        /// <summary>
        /// 在目标表格中添加一行数据
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="colnames">列名s</param>
        /// <param name="values">需要添加的数据s</param>
        public static void AddRowToTable(string tableName, string[] colnames, string[] values)
        {
            //获取列名
            string colname = "";
            for (int i = 0; i < colnames.Length; i++)
            {
                colname += $"{colnames[i]},";
            }
            colname = colname.Trim(',');

            //获取要输入的数据
            string vals = "";
            for (int i = 0; i < values.Length; i++)
            {
                vals += "'" + values[i] + "',";
            }

            //去掉数据中的非法字符
            vals = vals.Replace(" ", "_");
            vals = vals.Replace("-", "_");
            vals = vals.Trim(',');

            //生成插入命令
            string commandStr = $"insert into {tableName}({colname}) values ({vals})";

            ExecuteSQL(commandStr);
        }
        #endregion

        #region privateFunc
        private static void ConnectToSQL()
        {

            if (con.State != ConnectionState.Open)
            {
                con.ConnectionString = connectionStr;
                con.Open();
            }

        }
        private static void DisconnectSQL()
        {
            if (con.State == ConnectionState.Open)
            {
                con.Close();
            }
        }
        #endregion

    }
}
