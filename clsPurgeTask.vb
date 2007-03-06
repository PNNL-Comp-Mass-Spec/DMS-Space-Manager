Imports System.Collections.Specialized
Imports System.Data.SqlClient
Imports SpaceManagerNet.MgrSettings
Imports PRISM.Logging

Namespace PurgeTask

#Region "Interfaces"
	Public Interface IPurgeTaskParams
		'Used for job closeout
		Enum CloseOutType
			CLOSEOUT_SUCCESS = 0
			CLOSEOUT_FAILED = 1
			CLOSEOUT_UPDATE_REQUIRED = 2
		End Enum

		Function GetParam(ByVal Name As String) As String
	End Interface
#End Region

	Public Class clsPurgeTask
		Inherits clsDBTask
		Implements IPurgeTaskParams

#Region "Member Variables"
		' job parameters returned from request
		Private m_jobParams As New StringDictionary
		'Debug level
#End Region

#Region "parameters for calling stored procedures"
		' request parameters
		'Private mp_StorageServerName As String
		'Private mp_dataset As String
		'Private mp_DatasetID As Int32
		'Private mp_Folder As String
		'Private mp_StorageVol As String
		'Private mp_storagePath As String
		'Private mp_StorageVolExternal As String
		'Private mp_RawDataType As String
		'Private mp_ParamList As String
		'Private mp_message As String
#End Region

		Public Sub New(ByVal mgrParams As IMgrParams, ByVal logger As ILogger, ByVal DebugLevel As Integer)

			' constructor
			MyBase.New(mgrParams, logger, DebugLevel)

		End Sub

		Public Function RequestTask(ByVal MachName As String, ByVal drive As String) As Boolean

			Dim RetVal As clsDBTask.RequestTaskResult = clsDBTask.RequestTaskResult.NoTaskFound

			OpenConnection()
			RetVal = RequestPurgeTask(MachName, drive)
			If m_DebugLevel > 3 Then
				m_logger.PostEntry("clsPurgeTask.RequestTask: RetVal=" & RetVal.ToString, ILogger.logMsgType.logDebug, True)
			End If
			CLoseConnection()
			If RetVal = clsDBTask.RequestTaskResult.ResultError Then
				'There was an error
				m_logger.PostEntry("Error during purge task request", ILogger.logMsgType.logError, True)
				m_TaskWasAssigned = False
			ElseIf RetVal = clsDBTask.RequestTaskResult.NoTaskFound Then
				m_logger.PostEntry("No purge task found", ILogger.logMsgType.logNormal, True)
				m_TaskWasAssigned = False
			Else
				m_TaskWasAssigned = True
			End If

			If m_DebugLevel > 3 Then
				m_logger.PostEntry("clsPurgeTask.RequestTask: m_TaskWasAssigned=" & m_TaskWasAssigned.ToString, ILogger.logMsgType.logDebug, True)
			End If

			Return m_TaskWasAssigned

		End Function

		Public Sub CloseTask(ByVal closeOut As IPurgeTaskParams.CloseOutType, ByVal comment As String)

			OpenConnection()
			SetPurgeTaskComplete(GetCompletionCode(closeOut), comment)
			CLoseConnection()

		End Sub

		Private Function GetCompletionCode(ByVal closeOut As IPurgeTaskParams.CloseOutType) As Integer

			Dim code As Integer = 1			 '  0->success, 1->failure, anything else ->no intermediate files
			Select Case closeOut
				Case IPurgeTaskParams.CloseOutType.CLOSEOUT_SUCCESS
					code = 0
				Case IPurgeTaskParams.CloseOutType.CLOSEOUT_FAILED
					code = 1
				Case IPurgeTaskParams.CloseOutType.CLOSEOUT_UPDATE_REQUIRED
					code = 2
			End Select
			GetCompletionCode = code

		End Function

		'-------[for interface IPurgeTaskParams]----------------------------------------------
		Public Function GetParam(ByVal Name As String) As String Implements IPurgeTaskParams.GetParam

			Return m_jobParams(Name)

		End Function

		Private Function RequestPurgeTask(ByVal MachName As String, ByVal drive As String) As clsDBTask.RequestTaskResult

			Dim sc As SqlCommand
			Dim Outcome As clsDBTask.RequestTaskResult = clsDBTask.RequestTaskResult.NoTaskFound
			Dim RetryCount As Integer = 0

			m_error_list.Clear()

			' create the command object
			'
			sc = New SqlCommand("RequestPurgeTask", m_DBCn)
			sc.CommandType = CommandType.StoredProcedure

			' define parameters for command object
			'
			Dim myParm As SqlParameter
			'
			' define parameter for stored procedure's return value
			'
			myParm = sc.Parameters.Add("@Return", SqlDbType.Int)
			myParm.Direction = ParameterDirection.ReturnValue
			'
			' define parameters for the stored procedure's arguments
			'
			myParm = sc.Parameters.Add("@StorageServerName", SqlDbType.VarChar, 64)
			myParm.Direction = ParameterDirection.Input
			myParm.Value = MachName

			myParm = sc.Parameters.Add("@dataset", SqlDbType.VarChar, 128)
			myParm.Direction = ParameterDirection.Output

			myParm = sc.Parameters.Add("@DatasetID", SqlDbType.Int)
			myParm.Direction = ParameterDirection.Output

			myParm = sc.Parameters.Add("@Folder", SqlDbType.VarChar, 256)
			myParm.Direction = ParameterDirection.Output

			myParm = sc.Parameters.Add("@StorageVol", SqlDbType.VarChar, 256)
			myParm.Direction = ParameterDirection.InputOutput
			myParm.Value = drive & "\"

			myParm = sc.Parameters.Add("@storagePath", SqlDbType.VarChar, 256)
			myParm.Direction = ParameterDirection.Output

			myParm = sc.Parameters.Add("@StorageVolExternal", SqlDbType.VarChar, 256)
			myParm.Direction = ParameterDirection.Output

			myParm = sc.Parameters.Add("@RawDataType", SqlDbType.VarChar, 32)
			myParm.Direction = ParameterDirection.Output

			myParm = sc.Parameters.Add("@ParamList", SqlDbType.VarChar, 1024)
			myParm.Direction = ParameterDirection.Output

			myParm = sc.Parameters.Add("@message", SqlDbType.VarChar, 512)
			myParm.Direction = ParameterDirection.Output

			While RetryCount < 5
				Try
					' execute the stored procedure
					'
					sc.ExecuteNonQuery()

					' get return value
					'
					Dim ret As Integer
					ret = CInt(sc.Parameters("@Return").Value)

					If ret = 0 Then
						'No errors found in SP call, se see if any purge tasks were found
						Dim DsID As Integer = CInt(sc.Parameters("@DatasetID").Value)
						If DsID <> 0 Then
							'Purge task was found; get the data for it
							If AddJobParamsToDictionary(CInt(sc.Parameters("@DatasetID").Value)) Then
								Outcome = clsDBTask.RequestTaskResult.TaskFound
							Else
								Outcome = clsDBTask.RequestTaskResult.ResultError
							End If							 'Addition of parameters to dictionary
						Else
							'No jobs found
							Outcome = clsDBTask.RequestTaskResult.NoTaskFound
						End If				 'DsID check
					Else
						'There was an SP error
						Dim msg As String = "clsPurgeTask.RequestPurgeTask(), SP execution error " & ret.ToString
						m_logger.PostEntry(msg, ILogger.logMsgType.logError, True)
						Outcome = clsDBTask.RequestTaskResult.ResultError
					End If				  'Return value check

					Exit While
				Catch SqEx As SqlException
					If SqEx.Message.LastIndexOf("deadlock") > 1 Then
						'Error caused by deadlock; wait a few seconds and try again
						RetryCount += 1
						m_logger.PostEntry("Deadlock occurred requesting task; Retry count: " & RetryCount.ToString _
						 & ". Message: " & SqEx.Message, ILogger.logMsgType.logWarning, True)
						System.Threading.Thread.Sleep(3000)
					Else
						m_logger.PostError("Error requesting task: ", SqEx, True)
						Outcome = clsDBTask.RequestTaskResult.ResultError
						Exit While
					End If
				Catch ex As System.Exception
					m_logger.PostError("Error requesting task: ", ex, True)
					Outcome = clsDBTask.RequestTaskResult.ResultError
					Exit While
				End Try
			End While

			LogErrorEvents()

			Return Outcome

		End Function

		Private Function SetPurgeTaskComplete(ByVal completionCode As Int32, ByRef message As String) As Boolean
			Dim sc As SqlCommand
			Dim Outcome As Boolean = False

			Try
				m_error_list.Clear()

				' create the command object
				'
				sc = New SqlCommand("SetPurgeTaskComplete", m_DBCn)
				sc.CommandType = CommandType.StoredProcedure

				' define parameters for command object
				'
				Dim myParm As SqlParameter
				'
				' define parameter for stored procedure's return value
				'
				myParm = sc.Parameters.Add("@Return", SqlDbType.Int)
				myParm.Direction = ParameterDirection.ReturnValue
				'
				' define parameters for the stored procedure's arguments
				'
				myParm = sc.Parameters.Add("@datasetNum", SqlDbType.VarChar, 128)
				myParm.Direction = ParameterDirection.Input
				myParm.Value = m_jobParams("dataset")

				myParm = sc.Parameters.Add("@completionCode", SqlDbType.Int)
				myParm.Direction = ParameterDirection.Input
				myParm.Value = completionCode

				myParm = sc.Parameters.Add("@message", SqlDbType.VarChar, 512)
				myParm.Direction = ParameterDirection.Output

				' execute the stored procedure
				'
				sc.ExecuteNonQuery()

				' get return value
				'
				Dim ret As Object
				ret = sc.Parameters("@Return").Value

				' get values for output parameters
				'
				message = CStr(sc.Parameters("@message").Value)

				' if we made it this far, we succeeded
				'
				Outcome = True

			Catch ex As System.Exception
				m_logger.PostError("Error closing task: ", ex, True)
				Outcome = False
			End Try

			LogErrorEvents()

			Return Outcome

		End Function

		Private Function AddJobParamsToDictionary(ByVal DatasetID As Integer) As Boolean

			'Finds the archive job parameters for the specified Dataset ID and loads them into the parameters dictionary
			Dim Msg As String

			Dim SQLStr As String = "SELECT * FROM V_RequestPurgeTask WHERE DatasetID = " & DatasetID.ToString
			Dim ResultTable As DataTable = GetJobParamsFromTableWithRetries(SQLStr)
			If ResultTable Is Nothing Then			'There was an error
				Msg = "clsPurgeTask.AddJobParamsToDictionary(), Unable to obtain task data"
				m_logger.PostEntry(Msg, ILogger.logMsgType.logError, True)
				Return False
			End If
			'Verify exactly 1 row was received
			If ResultTable.Rows.Count <> 1 Then
				Msg = "clsPurgeTask.AddJobParamsToDictionary(), Invalid job data record count: " & ResultTable.Rows.Count.ToString
				m_logger.PostEntry(Msg, ILogger.logMsgType.logError, True)
				Return False
			End If

			'Load job parameters into dictionary
			Try
				Dim ResRow As DataRow = ResultTable.Rows(0)
				m_jobParams("dataset") = DbCStr(ResRow(ResultTable.Columns("dataset")))
				m_jobParams("datasetid") = DbCStr(ResRow(ResultTable.Columns("datasetid")))
				m_jobParams("datasetfolder") = DbCStr(ResRow(ResultTable.Columns("Folder")))
				m_jobParams("SambaStoragePath") = DbCStr(ResRow(ResultTable.Columns("SambaStoragePath")))
				m_jobParams("StorageServerName") = DbCStr(ResRow(ResultTable.Columns("StorageServerName")))
				m_jobParams("StorageVol") = DbCStr(ResRow(ResultTable.Columns("StorageVol")))
				m_jobParams("storagePath") = DbCStr(ResRow(ResultTable.Columns("storagePath")))
				m_jobParams("StorageVolExternal") = DbCStr(ResRow(ResultTable.Columns("StorageVolExternal")))
				Return True
			Catch ex As Exception
				Msg = "clsPurgeTask.AddJobParamsToDictionary(), Exception loading job params: " & ex.Message
				m_logger.PostEntry(Msg, ILogger.logMsgType.logError, True)
				Return False
			End Try

		End Function

	End Class

End Namespace
