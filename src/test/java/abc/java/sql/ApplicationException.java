package abc.java.sql;

public final class ApplicationException extends RuntimeException
{
   private static final long serialVersionUID = -5546821021865504159L;

   private ApplicationException()
   {
      super();
   }

   private ApplicationException(String message)
   {
      super(message);
   }

   public static RuntimeException create(String message)
   {
      return new ApplicationException(message);
   }
}
