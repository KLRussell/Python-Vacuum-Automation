from Vacuum_Global import settings
from Vacuum_Global import SQLConnect
from Vacuum_Global import validatecol
from Vacuum_Global import processresults
from Vacuum_Global import writelog
from Vacuum_Global import defaultheader
from Vacuum_Global import getbatch
import pandas as pd


class DisputeActions:
    def __init__(self, action, folder_name, asql=None, df=pd.DataFrame()):
        self.action = action
        self.df = df
        self.folder_name = folder_name
        self.asql = asql

        if not self.df.empty:
            self.df = defaultheader(self.df, '''dispute_id, action, amount_or_days, credit_invoice_date
                , action_norm_reason, action_reason, assign_rep, note_tag, attachment, ilec_confirmation
                , error_columns, error_message''')

    def escalate(self):
        self.asql.execute('''
            update A
                set
                    A.Error_Columns = 'DSB_ID',
                    A.Error_Message = 'DSB_ID does not exist in Dispute Current'

            FROM mydisputes As A
            left join {0} As B
            on
                A.DSB_ID = B.DSB_ID

            where
                B.DSB_ID is null'''.format(settings['Dispute_Current']))

        if self.asql.query("select object_id('DH')").iloc[0, 0]:
            self.asql.execute("DROP TABLE DH")

        self.asql.execute("CREATE TABLE DH (DH_ID int, DSB_ID int)")

        self.asql.execute('''
            INSERT INTO {0}
            (
                DSB_ID,
                Dispute_Category,
                Display_Status,
                Date_Submitted,
                ILEC_Confirmation,
                ILEC_Comments,
                Escalate,
                Escalate_DT,
                Escalate_Amount,
                Dispute_Reason,
                STC_Index,
                GRT_Update_Rep,
                Resolution_Date,
                Date_Updated,
                Source_File
            )
            OUTPUT
                INSERTED.DH_ID,
                INSERTED.DSB_ID
                
            INTO
                DH

            SELECT
                A.DSB_ID,
                C.Dispute_Category,
                'Filed',
                C.Date_Submitted,
                C.ILEC_Confirmation,
                C.ILEC_Comments,
                'Y',
                cast(getdate() as date),
                C.Dispute_Amount,
                A.Action_Reason,
                C.STC_Index,
                B.Full_Name,
                C.Resolution_Date,
                getdate(),
                'GRT Status: {3}'

            FROM mydisputes As A
            INNER JOIN {1} As B
            ON
                A.Comp_Serial = B.Comp_Serial
            inner join {2} As C
            on
                A.DSB_ID = C.DSB_ID
        '''.format(settings['Dispute_History'], settings['CAT_Emp'], settings['Dispute_Current'], getbatch()))

        self.asql.execute('''
            UPDATE B
            SET
                B.Display_Status = 'Filed',
                B.Escalate_Date = cast(getdate() as date),
                B.#_Of_Escalations = case when B.#_Of_Escalations is null then 1 else B.#_Of_Escalations + 1 end,
                B.Dispute_Reason = A.Action_Reason,
                B.Last_GRT_Action = 'Filed',
                B.Last_GRT_Action_Rep = C.Full_Name,
                B.Date_Updated = getdate(),
                B.Source_File = 'GRT Status: {2}',
                B.DH_ID = DH.DH_ID

            FROM mydisputes As A
            INNER JOIN {0} As B
            ON
                A.DSB_ID = B.DSB_ID
            INNER JOIN {1} As C
            ON
                A.Comp_Serial = C.Comp_Serial
            INNER JOIN DH
            ON
                A.DSB_ID = DH.DSB_ID
        '''.format(settings['Dispute_Current'], settings['CAT_Emp'], getbatch()))

        self.asql.execute('DROP TABLE DH')

    def close(self):
        self.asql.execute('''
            update A
                set
                    A.Error_Columns = 'DSB_ID',
                    A.Error_Message = 'DSB_ID does not exist in Dispute Current'

            FROM mydisputes As A
            left join {0} As B
            on
                A.DSB_ID = B.DSB_ID

            where
                B.DSB_ID is null'''.format(settings['Dispute_Current']))

        if self.asql.query("select object_id('DH')").iloc[0, 0]:
            self.asql.execute("DROP TABLE DH")

        self.asql.execute("CREATE TABLE DH (DH_ID int, DSB_ID int)")

        self.asql.execute('''
            INSERT INTO {0}
            (
                DSB_ID,
                Dispute_Category,
                Display_Status,
                Date_Submitted,
                ILEC_Confirmation,
                ILEC_Comments,
                Escalate,
                Escalate_DT,
                Escalate_Amount,
                Dispute_Reason,
                STC_Index,
                Norm_Close_Reason,
                Close_Reason,
                GRT_Update_Rep,
                Resolution_Date,
                Date_Updated,
                Source_File
            )
            OUTPUT
                INSERTED.DH_ID,
                INSERTED.DSB_ID

            INTO
                DH

            SELECT
                A.DSB_ID,
                D.Dispute_Category,
                'Denied - Closed',
                D.Date_Submitted,
                D.ILEC_Confirmation,
                D.ILEC_Comments,
                D.Escalate,
                D.Escalate_DT,
                D.Escalate_Amount,
                D.Dispute_Reason,
                D.STC_Index,
                A.Action_Norm_Reason,
                A.Action_Reason,
                B.Full_Name,
                D.Resolution_Date,
                getdate(),
                'GRT Status: {3}'

            FROM mydisputes As A
            INNER JOIN {1} As B
            ON
                A.Comp_Serial = B.Comp_Serial
            inner join {2} As C
            on
                A.DSB_ID = C.DSB_ID
            inner join {0} As D
            on
                D.DH_ID = C.DH_ID
            
            where
                A.Error_Columns is null
        '''.format(settings['Dispute_History'], settings['CAT_Emp'], settings['Dispute_Current'], getbatch()))

        self.asql.execute('''
            UPDATE B
                SET
                    B.Display_Status = 'Denied - Closed',
                    B.Norm_Close_Reason = A.Action_Norm_Reason,
                    B.Close_Reason = A.Action_Reason,
                    B.Last_GRT_Action = 'GRT Escalate',
                    B.Last_GRT_Action_Rep = C.Full_Name,
                    B.Date_Updated = getdate(),
                    B.Source_File = 'GRT Status: {2}',
                    B.DH_ID = DH.DH_ID

            FROM mydisputes As A
            INNER JOIN {0} As B
            ON
                A.DSB_ID = B.DSB_ID
            INNER JOIN {1} As C
            ON
                A.Comp_Serial = C.Comp_Serial
            INNER JOIN DH
            ON
                A.DSB_ID = DH.DSB_ID
            
            where
                A.Error_Columns is null
        '''.format(settings['Dispute_Current'], settings['CAT_Emp'], getbatch()))

        self.asql.execute('DROP TABLE DH')

    def paid(self):
        self.asql.execute('''
            update A
                set
                    A.Error_Columns = 'DSB_ID',
                    A.Error_Message = 'DSB_ID does not exist in Dispute Current'

            FROM mydisputes As A
            left join {0} As B
            on
                A.DSB_ID = B.DSB_ID

            where
                B.DSB_ID is null'''.format(settings['Dispute_Current']))

        if not self.df.empty:
            validatecol(self.asql, 'mydisputes', 'Amount_Or_Days')
            validatecol(self.asql, 'mydisputes', 'Credit_Invoice_Date', True)

        if self.asql.query("select object_id('DH')").iloc[0, 0]:
            self.asql.execute("DROP TABLE DH")

        self.asql.execute("CREATE TABLE DH (DH_ID int, DSB_ID int)")

        self.asql.execute('''
            INSERT INTO {0}
            (
                DSB_ID,
                Dispute_Category,
                Display_Status,
                Date_Submitted,
                ILEC_Confirmation,
                ILEC_Comments,
                Credit_Approved,
                Denied,
                Credit_Received_Amount,
                Credit_Received_Invoice_Date,
                Escalate,
                Escalate_DT,
                Escalate_Amount,
                Dispute_Reason,
                STC_Index,
                GRT_Update_Rep,
                Resolution_Date,
                Date_Updated,
                Source_File
            )
            OUTPUT
                INSERTED.DH_ID,
                INSERTED.DSB_ID

            INTO
                DH

            SELECT
                A.DSB_ID,
                D.Dispute_Category,
                'Paid',
                D.Date_Submitted,
                D.ILEC_Confirmation,
                D.ILEC_Comments,
                A.Amount_Or_Days,
                D.Denied,
                A.Amount_Or_Days,
                A.Credit_Invoice_Date,
                D.Escalate,
                D.Escalate_DT,
                D.Escalate_Amount,
                D.Dispute_Reason,
                D.STC_Index,
                B.Full_Name,
                D.Resolution_Date,
                getdate(),
                'GRT Status: {3}'

            FROM mydisputes As A
            INNER JOIN {1} As B
            ON
                A.Comp_Serial = B.Comp_Serial
            inner join {2} As C
            on
                A.DSB_ID = C.DSB_ID
            inner join {0} As D
            on
                D.DH_ID = C.DH_ID
            
            where
                Error_Columns is null
        '''.format(settings['Dispute_History'], settings['CAT_Emp'], settings['Dispute_Current'], getbatch()))

        self.asql.execute('''
            UPDATE B
            SET
                B.Display_Status = 'Paid',
                B.Credit_Approved = A.Amount_Or_Days,
                B.Credit_Received_Amount = A.Amount_Or_Days,
                B.Credit_Received_Invoice_Date = A.Credit_Invoice_Date,
                B.Last_GRT_Action = 'Paid',
                B.Last_GRT_Action_Rep = C.Full_Name,
                B.Date_Updated = getdate(),
                B.Source_File = 'GRT Status: {2}',
                B.DH_ID = DH.DH_ID

            FROM mydisputes As A
            INNER JOIN {0} As B
            ON
                A.DSB_ID = B.DSB_ID
            INNER JOIN {1} As C
            ON
                A.Comp_Serial = C.Comp_Serial
            INNER JOIN DH
            ON
                B.DSB_ID = DH.DSB_ID
                
            where
                Error_Columns is null
        '''.format(settings['Dispute_Current'], settings['CAT_Emp'], getbatch()))

        self.asql.execute('DROP TABLE DH')

    def denied(self):
        self.asql.execute('''
            update A
                set
                    A.Error_Columns = 'DSB_ID',
                    A.Error_Message = case
                        when B.DSB_ID is null then 'DSB_ID does not exist in Dispute Current'
                        else 'Dispute_Type is not Email for DSB_ID'
                    end

            FROM mydisputes As A
            left join {0} As B
            on
                A.DSB_ID = B.DSB_ID

            where
                B.DSB_ID is null
                    or
                B.Dispute_Type != 'Email'
        '''.format(settings['Dispute_Current']))

        if self.asql.query("select object_id('DH')").iloc[0, 0]:
            self.asql.execute("DROP TABLE DH")

        self.asql.execute("CREATE TABLE DH (DH_ID int, DSB_ID int)")

        self.asql.execute('''
            INSERT INTO {0}
            (
                DSB_ID,
                Dispute_Category,
                Display_Status,
                Date_Submitted,
                ILEC_Confirmation,
                ILEC_Comments,
                Escalate,
                Escalate_DT,
                Escalate_Amount,
                Dispute_Reason,
                STC_Index,
                GRT_Update_Rep,
                Resolution_Date,
                Date_Updated,
                Source_File
            )
            OUTPUT
                INSERTED.DH_ID,
                INSERTED.DSB_ID

            INTO
                DH

            SELECT
                A.DSB_ID,
                D.Dispute_Category,
                'Approved',
                D.Date_Submitted,
                A.ILEC_Confirmation,
                A.Action_Reason,
                D.Escalate,
                D.Escalate_DT,
                D.Escalate_Amount,
                D.Dispute_Reason,
                D.STC_Index,
                B.Full_Name,
                D.Resolution_Date,
                getdate(),
                'GRT Status: {3}'

            FROM mydisputes As A
            INNER JOIN {1} As B
            ON
                A.Comp_Serial = B.Comp_Serial
            inner join {2} As C
            on
                A.DSB_ID = C.DSB_ID
            inner join {0} As D
            on
                D.DH_ID = C.DH_ID

            where
                Error_Columns is null
        '''.format(settings['Dispute_History'], settings['CAT_Emp'], settings['Dispute_Current'], getbatch()))

        self.asql.execute('''
            UPDATE B
            SET
                B.Display_Status = 'Denied - Pending',
                B.ILEC_Confirmation = A.ILEC_Confirmation,
                B.ILEC_Comments = A.Action_Reason,
                B.Last_GRT_Action = 'Denied - Pending',
                B.Last_GRT_Action_Rep = C.Full_Name,
                B.Date_Updated = getdate(),
                B.Source_File = 'GRT Status: {2}',
                B.DH_ID = DH.DH_ID

            FROM mydisputes As A
            INNER JOIN {0} As B
            ON
                A.DSB_ID = B.DSB_ID
            INNER JOIN {1} As C
            ON
                A.Comp_Serial = C.Comp_Serial
            INNER JOIN DH
            ON
                A.DSB_ID = DH.DSB_ID

            where
                Error_Columns is null
        '''.format(settings['Dispute_Current'], settings['CAT_Emp'], getbatch()))

        self.asql.execute('DROP TABLE DH')

    def approved(self):
        validatecol(self.asql, 'mydisputes', 'Amount_Or_Days')

        self.asql.execute('''
            update A
                set
                    A.Error_Columns = 'DSB_ID',
                    A.Error_Message = case
                        when B.DSB_ID is null then 'DSB_ID does not exist in Dispute Current'
                        else 'Dispute_Type is not Email for DSB_ID'
                    end
                    
            FROM mydisputes As A
            left join {0} As B
            on
                A.DSB_ID = B.DSB_ID
            
            where
                B.DSB_ID is null
                    or
                B.Dispute_Type != 'Email'
        '''.format(settings['Dispute_Current']))

        if self.asql.query("select object_id('DH')").iloc[0, 0]:
            self.asql.execute("DROP TABLE DH")

        self.asql.execute("CREATE TABLE DH (DH_ID int, DSB_ID int)")

        self.asql.execute('''
            INSERT INTO {0}
            (
                DSB_ID,
                Dispute_Category,
                Display_Status,
                Date_Submitted,
                ILEC_Confirmation,
                ILEC_Comments,
                Credit_Approved,
                Denied,
                Escalate,
                Escalate_DT,
                Escalate_Amount,
                Dispute_Reason,
                STC_Index,
                GRT_Update_Rep,
                Resolution_Date,
                Date_Updated,
                Source_File
            )
            OUTPUT
                INSERTED.DH_ID,
                INSERTED.DSB_ID

            INTO
                DH

            SELECT
                A.DSB_ID,
                D.Dispute_Category,
                'Approved',
                D.Date_Submitted,
                A.ILEC_Confirmation,
                A.Action_Reason,
                A.Amount_Or_Days,
                D.Denied,
                D.Escalate,
                D.Escalate_DT,
                D.Escalate_Amount,
                D.Dispute_Reason,
                D.STC_Index,
                B.Full_Name,
                D.Resolution_Date,
                getdate(),
                'GRT Status: {3}'

            FROM mydisputes As A
            INNER JOIN {1} As B
            ON
                A.Comp_Serial = B.Comp_Serial
            inner join {2} As C
            on
                A.DSB_ID = C.DSB_ID
            inner join {0} As D
            on
                D.DH_ID = C.DH_ID

            where
                Error_Columns is null
        '''.format(settings['Dispute_History'], settings['CAT_Emp'], settings['Dispute_Current'], getbatch()))

        self.asql.execute('''
            UPDATE B
            SET
                B.Display_Status = 'Approved',
                B.Credit_Approved = A.Amount_Or_Days,
                B.Last_GRT_Action = 'Approved',
                B.Last_GRT_Action_Rep = C.Full_Name,
                B.Date_Updated = getdate(),
                B.Source_File = 'GRT Status: {2}',
                B.DH_ID = DH.DH_ID

            FROM mydisputes As A
            INNER JOIN {0} As B
            ON
                A.DSB_ID = B.DSB_ID
            INNER JOIN {1} As C
            ON
                A.Comp_Serial = C.Comp_Serial
            INNER JOIN DH
            ON
                A.DSB_ID = DH.DSB_ID

            where
                Error_Columns is null
        '''.format(settings['Dispute_Current'], settings['CAT_Emp'], getbatch()))

        self.asql.execute('DROP TABLE DH')

    def disputenote(self):
        self.asql.execute('''
            update A
                set
                    A.Error_Columns = 'DSB_ID',
                    A.Error_Message = 'DSB_ID does not exist in Dispute Current'

            FROM mydisputes As A
            left join {0} As B
            on
                A.DSB_ID = B.DSB_ID

            where
                B.DSB_ID is null'''.format(settings['Dispute_Current']))

        if not self.df.empty:
            validatecol(self.asql, 'mydisputes', 'Amount_Or_Days')

        self.asql.execute('''
            insert into {0}
            (
                DSB_ID,
                Logged_By,
                Norm_Note_Action,
                Dispute_Note,
                Days_Till_Action,
                Assign_To,
                Note_Tag,
                Attachment
            )
            select
                A.DSB_ID,
                B.Full_Name,
                A.Action_Norm_Reason,
                A.Action_Reason,
                A.Amount_Or_Days,
                A.Assign_Rep,
                A.Note_Tag,
                A.Attachment

            from mydisputes As A
            INNER JOIN {1} As B
            ON
                A.Comp_Serial = B.Comp_Serial
                
            where
                Error_Columns is null
        '''.format(settings['Dispute_Notes'], settings['CAT_Emp']))

        self.asql.execute('''
            UPDATE B
            SET
                B.Norm_Note_Action = A.Action_Norm_Reason,
                B.Dispute_Note = A.Action_Reason,
                B.Note_Group_Tag = A.Note_Tag,
                B.Note_Assigned_To = case when A.Assign_Rep is not null then A.Assign_Rep else C.Full_Name end,
                B.Days_Till_Note_Review = A.Amount_Or_Days,
                B.Note_Added_On = getdate()

            FROM mydisputes As A
            INNER JOIN {0} As B
            ON
                A.DSB_ID = B.DSB_ID
            INNER JOIN {1} As C
            ON
                A.Comp_Serial = C.Comp_Serial
            
            where
                Error_Columns is null
        '''.format(settings['Dispute_Current'], settings['CAT_Emp']))

    def process(self):
        if not self.asql:
            writelog("Processing {0} GRT Dispute Actions".format(len(self.df)), 'info')

            self.asql = SQLConnect('alch')
            self.asql.connect()
            self.asql.upload(self.df, 'mydisputes')

        if self.action == 'Escalate':
            self.escalate()
        elif self.action == 'Close':
            self.close()
        elif self.action == 'Paid':
            validatecol(self.asql, 'mydisputes', 'Amount_Or_Days')
            validatecol(self.asql, 'mydisputes', 'Credit_Invoice_Date', True)
            self.paid()
        elif self.action == 'Denied':
            self.denied()
        elif self.action == 'Approved':
            self.approved()
        elif self.action == 'Dispute Note':
            self.disputenote()

        if not self.df.empty:
            processresults(self.folder_name, self.asql, 'mydisputes', 'New Seed Disputes')
            self.asql.close()
