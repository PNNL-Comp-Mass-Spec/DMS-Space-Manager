﻿
namespace Space_Manager
{
	public class clsHashInfo
	{
		private string mHashCode;
        private string mMyEMSLFileID;

		/// <summary>
		/// MD5 or Sha-1 Hash
		/// </summary>
		public string HashCode
		{
			get
			{
				return mHashCode;
			}
			set
			{
				mHashCode = value ?? string.Empty;
			}
		}

		public string MyEMSLFileID
		{
			get
			{
				return mMyEMSLFileID;
			}
			set
			{
				mMyEMSLFileID = value ?? string.Empty;
			}
		}

		// Constructor
		public clsHashInfo() :
			this(string.Empty, string.Empty) { }

		public clsHashInfo(string hashCode, string myEmslFileID)
		{
			this.Clear();
			this.HashCode = hashCode;
			this.MyEMSLFileID = myEmslFileID;
		}

		public void Clear()
		{
			HashCode = string.Empty;
			MyEMSLFileID = string.Empty;
		}

		public bool IsMatch(clsHashInfo comparisonValue)
		{
			if (string.Equals(this.HashCode, comparisonValue.HashCode) &&
				string.Equals(this.MyEMSLFileID, comparisonValue.MyEMSLFileID))
				return true;
			else
				return false;
		}

		public override string ToString()
		{
			string description;
			if (string.IsNullOrEmpty(HashCode))
				description = "#No Hash#";
			else
				description = HashCode;

			if (!string.IsNullOrEmpty(MyEMSLFileID))
				description += ", ID=" + MyEMSLFileID;

			return description;
		}

	}
}
