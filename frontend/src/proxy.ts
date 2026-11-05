import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";

const PUBLIC_PATHS = ["/auth/login", "/auth/register"];

export function proxy(request: NextRequest) {
  const { pathname } = request.nextUrl;
  const session = request.cookies.get("ledger_session");

  if (pathname.startsWith("/api")) {
    return NextResponse.next();
  }

  // The development-only component sheet (apply-markas-foundation D12) does not exist in other builds.
  if (pathname.startsWith("/komponen") && process.env.NEXT_PUBLIC_COMPONENT_SHEET !== "1") {
    return new NextResponse("Not Found", { status: 404 });
  }

  if (PUBLIC_PATHS.some((p) => pathname.startsWith(p))) {
    return NextResponse.next();
  }

  if (!session) {
    const loginUrl = new URL("/auth/login", request.url);
    loginUrl.searchParams.set("next", pathname);
    return NextResponse.redirect(loginUrl);
  }

  return NextResponse.next();
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"],
};
