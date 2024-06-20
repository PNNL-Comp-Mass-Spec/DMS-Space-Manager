
namespace Space_Manager
{
    public class HashInfo
    {
        // Ignore Spelling: EMSL

        private string mHashCode;
        private string mMyEMSLFileID;

        /// <summary>
        /// MD5 or SHA-1 Hash
        /// </summary>
        public string HashCode
        {
            get => mHashCode;
            set => mHashCode = value ?? string.Empty;
        }

        /// <summary>
        /// MyEMSL File ID
        /// </summary>
        public string MyEMSLFileID
        {
            get => mMyEMSLFileID;
            set => mMyEMSLFileID = value ?? string.Empty;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="hashCode"></param>
        /// <param name="myEmslFileID"></param>
        public HashInfo(string hashCode, string myEmslFileID)
        {
            Clear();
            HashCode = hashCode;
            MyEMSLFileID = myEmslFileID;
        }

        public void Clear()
        {
            HashCode = string.Empty;
            MyEMSLFileID = string.Empty;
        }

        public bool IsMatch(HashInfo comparisonValue)
        {
            return string.Equals(HashCode, comparisonValue.HashCode) &&
                   string.Equals(MyEMSLFileID, comparisonValue.MyEMSLFileID);
        }

        public override string ToString()
        {
            var description = string.IsNullOrEmpty(HashCode) ? "#No Hash#" : HashCode;

            if (!string.IsNullOrEmpty(MyEMSLFileID))
                return description + ", ID=" + MyEMSLFileID;

            return description;
        }
    }
}
