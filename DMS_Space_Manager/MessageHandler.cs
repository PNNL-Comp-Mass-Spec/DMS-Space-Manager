//*********************************************************************************************************
// Written by Gary Kiebel and Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2010, Battelle Memorial Institute
// Created 09/09/2010
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using Apache.NMS;
using Apache.NMS.ActiveMQ;
using Apache.NMS.ActiveMQ.Commands;
using PRISM.AppSettings;

namespace Space_Manager
{
    /// <summary>
    /// Handles sending and receiving of control and status messages
    /// </summary>
    internal class MessageHandler : LoggerBase, IDisposable
    {
        private MgrSettings m_MgrSettings;

        private IConnection m_Connection;
        private ISession m_StatusSession;
        private IMessageProducer m_StatusSender;

        private bool m_IsDisposed;
        private bool m_HasConnection;

        public MgrSettings MgrSettings
        {
            set => m_MgrSettings = value;
        }

        public string BrokerUri { get; set; }

        public string StatusTopicName { get; set; }

        /// <summary>
        /// Create set of NMS connection objects necessary to talk to the ActiveMQ broker
        /// </summary>
        /// <param name="retryCount">Number of times to try the connection</param>
        /// <param name="timeoutSeconds">Number of seconds to wait for the broker to respond</param>
        protected void CreateConnection(int retryCount = 2, int timeoutSeconds = 15)
        {
            if (m_HasConnection)
                return;

            if (retryCount < 0)
                retryCount = 0;

            var retriesRemaining = retryCount;

            if (timeoutSeconds < 5)
                timeoutSeconds = 5;

            var errorList = new List<string>();

            while (retriesRemaining >= 0)
            {
                try
                {
                    IConnectionFactory connectionFactory = new ConnectionFactory(BrokerUri);
                    m_Connection = connectionFactory.CreateConnection();
                    m_Connection.RequestTimeout = new TimeSpan(0, 0, timeoutSeconds);
                    m_Connection.Start();

                    m_HasConnection = true;

                    var username = System.Security.Principal.WindowsIdentity.GetCurrent().Name;

                    LogDebug(string.Format("Connected to broker as user {0}", username));

                    return;
                }
                catch (Exception ex)
                {
                    // Connection failed
                    if (!errorList.Contains(ex.Message))
                        errorList.Add(ex.Message);

                    // Sleep for 3 seconds
                    System.Threading.Thread.Sleep(3000);
                }

                retriesRemaining--;
            }

            // If we get here, we never could connect to the message broker

            var msg = "Exception creating broker connection";
            if (retryCount > 0)
                msg += " after " + (retryCount + 1) + " attempts";

            msg += ": " + string.Join("; ", errorList);

            LogError(msg);
        }

        /// <summary>
        /// Create the message broker communication objects and register the listener function
        /// </summary>
        /// <returns>TRUE for success; FALSE otherwise</returns>
        public bool Init()
        {
            try
            {
                if (!m_HasConnection)
                    CreateConnection();
                if (!m_HasConnection)
                    return false;

                if (string.IsNullOrWhiteSpace(StatusTopicName))
                {
                    LogWarning("Status topic queue name is undefined");
                }
                else
                {
                    // topic for the capture tool manager to send status information over
                    m_StatusSession = m_Connection.CreateSession();
                    m_StatusSender = m_StatusSession.CreateProducer(new ActiveMQTopic(StatusTopicName));
                    LogDebug("Status sender established");
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception while initializing message sessions", ex);
                DestroyConnection();
                return false;
            }
        }

        /// <summary>
        /// Sends a status message
        /// </summary>
        /// <param name="message">Outgoing message string</param>
        public void SendMessage(string message)
        {
            if (!m_IsDisposed)
            {
                var textMessage = m_StatusSession.CreateTextMessage(message);
                textMessage.NMSTimeToLive = TimeSpan.FromMinutes(60);
                textMessage.NMSDeliveryMode = MsgDeliveryMode.NonPersistent;
                textMessage.Properties.SetString("ProcessorName", m_MgrSettings.ManagerName);
                try
                {
                    m_StatusSender.Send(textMessage);
                }
                catch
                {
                    // Do nothing
                }
            }
            else
            {
                throw new ObjectDisposedException(GetType().FullName);
            }
        }

        /// <summary>
        /// Cleans up a connection after error or when closing
        /// </summary>
        protected void DestroyConnection()
        {
            if (m_HasConnection)
            {
                m_Connection.Dispose();
                m_HasConnection = false;
                LogDebug("Message connection closed");
            }
        }

        /// <summary>
        /// Implements IDisposable interface
        /// </summary>
        public void Dispose()
        {
            if (m_IsDisposed)
                return;

            DestroyConnection();
            m_IsDisposed = true;
        }
    }
}
