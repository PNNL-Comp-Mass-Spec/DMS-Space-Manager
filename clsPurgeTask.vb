Imports System.Collections.Specialized
Imports System.Data.SqlClient
Imports SpaceManagerNet.MgrSettings
Imports PRISM.Logging

Namespace PurgeTask

  Public Interface IPurgeTaskParams
    'Used for job closeout
    Enum CloseOutType
      CLOSEOUT_SUCCESS = 0
      CLOSEOUT_FAILED = 1
    End Enum

    Function GetParam(ByVal Name As String) As String
  End Interface

  Public Class clsPurgeTask
    Inherits clsDBTask
    Implements IPurgeTaskParams

#Region "Member Variables"

    ' job parameters returned from request
    Private m_jobParams As New StringDictionary()

#End Region

#Region "parameters for calling stored procedures"
    ' request parameters
    Private mp_StorageServerName As String
    Private mp_dataset As String
    Private mp_DatasetID As Int32
    Private mp_Folder As String
    Private mp_StorageVol As String
    Private mp_storagePath As String
		Private mp_StorageVolExternal As String
		Private mp_RawDataType As String
		Private mp_ParamList As String
    Private mp_message As String
#End Region

    ' constructor
    Public Sub New(ByVal mgrParams As IMgrParams, ByVal logger As ILogger)
      MyBase.New(mgrParams, logger)
      mp_StorageServerName = m_mgrParams.GetParam("ProgramControl", "MachName")
    End Sub

    Public Function RequestTask(ByVal drive As String) As Boolean
      OpenConnection()
      RequestPurgeTask(drive)
      CLoseConnection()
      m_TaskWasAssigned = (mp_DatasetID <> 0)
      Return m_TaskWasAssigned
    End Function

    Public Sub CloseTask(ByVal closeOut As IPurgeTaskParams.CloseOutType, ByVal comment As String)
      OpenConnection()
      SetPurgeTaskComplete(GetCompletionCode(closeOut), comment)
      CLoseConnection()
    End Sub

    Private Function GetCompletionCode(ByVal closeOut As IPurgeTaskParams.CloseOutType) As Integer
      Dim code As Integer = 1      '  0->success, 1->failure, anything else ->no intermediate files
      Select Case closeOut
        Case IPurgeTaskParams.CloseOutType.CLOSEOUT_SUCCESS
          code = 0
        Case IPurgeTaskParams.CloseOutType.CLOSEOUT_FAILED
          code = 1
      End Select
      GetCompletionCode = code
    End Function

    '-------[for interface IPurgeTaskParams]----------------------------------------------
    Public Function GetParam(ByVal Name As String) As String Implements IPurgeTaskParams.GetParam
      Dim s As String
      Select Case (Name)
        Case "StorageServerName"
          s = mp_StorageServerName
        Case "dataset"
          s = mp_dataset
        Case "DatasetID"
					s = mp_DatasetID.ToString
        Case "Folder"
          s = mp_Folder
        Case "StorageVol"
          s = mp_StorageVol
        Case "storagePath"
          s = mp_storagePath
        Case "StorageVolExternal"
					s = mp_StorageVolExternal
				Case "RawDataType"
					s = mp_RawDataType
				Case "ParamList"
					s = mp_ParamList
				Case "message"
					s = mp_message
			End Select
      Return s
    End Function

    '------[for DB access]-----------------------------------------------------------

    Private Function RequestPurgeTask(ByVal drive As String) As Boolean
      Dim sc As SqlCommand
      Dim Outcome As Boolean = False

      Try
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
        myParm.Value = mp_StorageServerName

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


        ' execute the stored procedure
        '
        sc.ExecuteNonQuery()

        ' get return value
        '
        Dim ret As Object
        ret = sc.Parameters("@Return").Value

        ' get values for output parameters
        '
				mp_dataset = CStr(sc.Parameters("@dataset").Value)
				mp_DatasetID = CInt(sc.Parameters("@DatasetID").Value)
				mp_Folder = CStr(sc.Parameters("@Folder").Value)
				mp_StorageVol = CStr(sc.Parameters("@StorageVol").Value)
				mp_StorageVolExternal = CStr(sc.Parameters("@StorageVolExternal").Value)
				mp_storagePath = CStr(sc.Parameters("@storagePath").Value)
				mp_RawDataType = CStr(sc.Parameters("@RawDataType").Value)
				mp_ParamList = CStr(sc.Parameters("@ParamList").Value)
				mp_message = CStr(sc.Parameters("@message").Value)

        ' if we made it this far, we succeeded
        '
        Outcome = True

      Catch ex As System.Exception
        m_logger.PostError("Error requesting task: ", ex, True)
        Outcome = False
      End Try

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
        myParm.Value = mp_dataset

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

  End Class

End Namespace
