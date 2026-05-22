@echo off

echo Be sure to compile in Release mode
pause

set TargetBase=\\Proto-3\DMS_Programs_Dist\CaptureTaskManagerDistribution\SpaceManager

echo Copying to %TargetBase%

@echo on
xcopy Release\net8.0-windows\Space_Manager.exe                %TargetBase% /D /Y
xcopy Release\net8.0-windows\Space_Manager.pdb                %TargetBase% /D /Y
xcopy Release\net8.0-windows\Space_Manager.exe.config         %TargetBase% /D /Y
xcopy Release\net8.0-windows\*.dll                            %TargetBase% /D /Y /S
xcopy Release\net8.0-windows\Space_Manager.deps.json          %TargetBase% /D /Y
xcopy Release\net8.0-windows\Space_Manager.runtimeconfig.json %TargetBase% /D /Y

xcopy ..\..\README.md                                         %TargetBase% /D /Y

:Done

echo.
if not "%1"=="NoPause" pause
