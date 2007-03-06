Imports System.Collections.Specialized
Imports System.Data.SqlClient
Imports SpaceManagerNet.MgrSettings
Imports PRISM.Logging

Public MustInherit Class clsDBTask

#Region "Enums"
	Public Enum RequestTaskResult As Byte
		TaskFound = 0
		NoTaskFound = 1
		ResultError = 2
	End Enum
#End Region

#Region "Member Variables"
	' access to the logger
	Protected m_logger As ILogger

	' access to mgr parameters
	Protected m_mgrParams As IMgrParams

	' DB access
	Protected m_connection_str As String
	Protected m_DBCn As SqlConnection
	Protected m_error_list As New StringCollection

	' job status
	Protected m_TaskWasAssigned As Boolean = False

	'Debug level
	Protected m_DebugLevel As Integer
#End Region

	Public Sub New(ByVal mgrParams As IMgrParams, ByVal logger As ILogger, ByVal DebugLevel As Integer)

		' constructor
		m_mgrParams = mgrParams
		m_logger = logger
		m_DebugLevel = DebugLevel
		m_connection_str = m_mgrParams.GetParam("DatabaseSettings", "ConnectionString")

	End Sub

	Public ReadOnly Property TaskWasAssigned() As Boolean

		Get
			Return m_TaskWasAssigned
		End Get

	End Property

	'------[for DB access]-----------------------------------------------------------

	Protected Sub OpenConnection()

		Dim retryCount As Integer = 3
		While retryCount > 0
			Try
				m_DBCn = New SqlConnection(m_connection_str)
				AddHandler m_DBCn.InfoMessage, New SqlInfoMessageEventHandler(AddressOf OnInfoMessage)
				m_DBCn.Open()
				retryCount = 0
			Catch e As SqlException
				retryCount -= 1
				m_DBCn.Close()
				m_logger.PostError("Connection problem: ", e, True)
				System.Threading.Thread.Sleep(300)
			End Try
		End While

	End Sub

	Protected Sub CLoseConnection()

		If Not m_DBCn Is Nothing Then
			m_DBCn.Close()
		End If

	End Sub

	Protected Sub LogErrorEvents()

		If m_error_list.Count > 0 Then
			m_logger.PostEntry("Warning messages were posted to local log", ILogger.logMsgType.logWarning, True)
		End If
		Dim s As String
		For Each s In m_error_list
			m_logger.PostEntry(s, ILogger.logMsgType.logWarning, True)
		Next

	End Sub

	Private Sub OnInfoMessage(ByVal sender As Object, ByVal args As SqlInfoMessageEventArgs)

		' event handler for InfoMessage event
		' errors and warnings sent from the SQL server are caught here
		Dim err As SqlError
		Dim s As String
		For Each err In args.Errors
			s = ""
			s &= "Message: " & err.Message
			s &= ", Source: " & err.Source
			s &= ", Class: " & err.Class
			s &= ", State: " & err.State
			s &= ", Number: " & err.Number
			s &= ", LineNumber: " & err.LineNumber
			s &= ", Procedure:" & err.Procedure
			s &= ", Server: " & err.Server
			m_error_list.Add(s)
		Next

	End Sub

	Protected Function GetJobParamsFromTableWithRetries(ByVal SqlStr As String) As DataTable

		'Requests job parameters from database. Input string specifies view to use. Performs retries if necessary.
		'Returns a data table containing results if successful, NOTHING on failure

		Dim RetryCount As Short = 3
		Dim ErrMsg As String

		'Get a table containing data for job
		Dim Cn As New SqlConnection(m_connection_str)
		Dim Da As New SqlDataAdapter(SqlStr, Cn)
		Dim Ds As DataSet = New DataSet

		While RetryCount > 0
			Try
				Da.Fill(Ds)
				Exit While
			Catch ex As Exception
				ErrMsg = "clsDBTask.GetJobParamsFromTableWithRetries(), Filling data adapter, " & ex.Message & "; Retry count = " & RetryCount.ToString
				m_logger.PostEntry(ErrMsg, ILogger.logMsgType.logError, True)
				RetryCount -= 1S
				System.Threading.Thread.Sleep(1000)				'Delay for 1 second before trying again
			End Try
		End While

		'If loop exited due to error, return nothing
		If RetryCount < 1 Then Return Nothing

		Return Ds.Tables(0)

	End Function

	Protected Function DbCStr(ByVal InpObj As Object) As String

		'If input object is DbNull, returns "", otherwise returns String representation of object
		If InpObj Is DBNull.Value Then
			Return ""
		Else
			Return CStr(InpObj)
		End If

	End Function

	Protected Function DbCSng(ByVal InpObj As Object) As Single

		'If input object is DbNull, returns 0.0, otherwise returns Single representation of object
		If InpObj Is DBNull.Value Then
			Return 0.0
		Else
			Return CSng(InpObj)
		End If

	End Function

	Protected Function DbCDbl(ByVal InpObj As Object) As Double

		'If input object is DbNull, returns 0.0, otherwise returns Double representation of object
		If InpObj Is DBNull.Value Then
			Return 0.0
		Else
			Return CDbl(InpObj)
		End If

	End Function

	Protected Function DbCInt(ByVal InpObj As Object) As Integer

		'If input object is DbNull, returns 0, otherwise returns Integer representation of object
		If InpObj Is DBNull.Value Then
			Return 0
		Else
			Return CInt(InpObj)
		End If

	End Function

	Protected Function DbCLng(ByVal InpObj As Object) As Long

		'If input object is DbNull, returns 0, otherwise returns Integer representation of object
		If InpObj Is DBNull.Value Then
			Return 0
		Else
			Return CLng(InpObj)
		End If

	End Function

	Protected Function DbCDec(ByVal InpObj As Object) As Decimal

		'If input object is DbNull, returns 0, otherwise returns Decimal representation of object
		If InpObj Is DBNull.Value Then
			Return 0
		Else
			Return CDec(InpObj)
		End If

	End Function

	Protected Function DbCShort(ByVal InpObj As Object) As Short

		'If input object is DbNull, returns 0, otherwise returns Short representation of object
		If InpObj Is DBNull.Value Then
			Return 0
		Else
			Return CShort(InpObj)
		End If

	End Function

End Class
