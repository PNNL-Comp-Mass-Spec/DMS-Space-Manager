

Public Class Form1
  Inherits System.Windows.Forms.Form

#Region " Windows Form Designer generated code "

	Public Sub New()
		MyBase.New()

		'This call is required by the Windows Form Designer.
		InitializeComponent()

		'Add any initialization after the InitializeComponent() call

	End Sub

	'Form overrides dispose to clean up the component list.
	Protected Overloads Overrides Sub Dispose(ByVal disposing As Boolean)
		If disposing Then
			If Not (components Is Nothing) Then
				components.Dispose()
			End If
		End If
		MyBase.Dispose(disposing)
	End Sub

	'Required by the Windows Form Designer
	Private components As System.ComponentModel.IContainer

	'NOTE: The following procedure is required by the Windows Form Designer
	'It can be modified using the Windows Form Designer.  
	'Do not modify it using the code editor.
	Friend WithEvents MainProcess As System.Windows.Forms.Button
	Friend WithEvents Splitter1 As System.Windows.Forms.Splitter
	Friend WithEvents lblStatus As System.Windows.Forms.Label
	<System.Diagnostics.DebuggerStepThrough()> Private Sub InitializeComponent()
		Me.MainProcess = New System.Windows.Forms.Button()
		Me.Splitter1 = New System.Windows.Forms.Splitter()
		Me.lblStatus = New System.Windows.Forms.Label()
		Me.SuspendLayout()
		'
		'MainProcess
		'
		Me.MainProcess.Location = New System.Drawing.Point(28, 19)
		Me.MainProcess.Name = "MainProcess"
		Me.MainProcess.Size = New System.Drawing.Size(88, 26)
		Me.MainProcess.TabIndex = 2
		Me.MainProcess.Text = "Main Process"
		'
		'Splitter1
		'
		Me.Splitter1.Name = "Splitter1"
		Me.Splitter1.Size = New System.Drawing.Size(4, 201)
		Me.Splitter1.TabIndex = 3
		Me.Splitter1.TabStop = False
		'
		'lblStatus
		'
		Me.lblStatus.BorderStyle = System.Windows.Forms.BorderStyle.FixedSingle
		Me.lblStatus.Location = New System.Drawing.Point(30, 66)
		Me.lblStatus.Name = "lblStatus"
		Me.lblStatus.Size = New System.Drawing.Size(198, 18)
		Me.lblStatus.TabIndex = 4
		'
		'Form1
		'
		Me.AutoScaleBaseSize = New System.Drawing.Size(5, 13)
		Me.ClientSize = New System.Drawing.Size(346, 201)
		Me.Controls.AddRange(New System.Windows.Forms.Control() {Me.lblStatus, Me.Splitter1, Me.MainProcess})
		Me.Name = "Form1"
		Me.Text = "Form1"
		Me.ResumeLayout(False)

	End Sub

#End Region

	Dim WithEvents mp As New clsMainProcess()

	Private Sub MainProcess_Click(ByVal sender As System.Object, ByVal e As System.EventArgs) Handles MainProcess.Click
		lblStatus.Text = "Beginning space management"
		mp.DoSpaceManagement()
	End Sub


	Private Sub mp_StatusChange(ByVal NewStatus As String) Handles mp.StatusChange
		lblStatus.Text = "Space management complete"
	End Sub

End Class
